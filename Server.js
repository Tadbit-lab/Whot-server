
// ============================================================================
// --- LOCAL SQLITE MIRROR DATABASE ENGINE (0ms LATENCY / ZERO FIRESTORE QUOTA) ---
// ============================================================================
let sqliteDb = null;
try {
  const Database = require('better-sqlite3');
  sqliteDb = new Database(require('path').join(__dirname, 'whot_local_mirror.db'));
  sqliteDb.exec(`
    CREATE TABLE IF NOT EXISTS players (
      uid TEXT PRIMARY KEY,
      name TEXT,
      photoURL TEXT,
      wins INTEGER DEFAULT 0,
      losses INTEGER DEFAULT 0,
      lastSeen DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS matches (
      id TEXT PRIMARY KEY,
      winner TEXT,
      playedAt DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE TABLE IF NOT EXISTS audit_logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      event TEXT,
      details TEXT,
      timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
    );
  `);
  console.log('⚡ Local SQLite mirror database initialized successfully!');
} catch (err) {
  console.warn('⚠️ SQLite initialization notice:', err.message);
}

// Helper Functions
function sqliteUpsertPlayer(uid, name, photoURL = '', wins = 0, losses = 0) {
  if (!sqliteDb || !uid) return;
  try {
    const stmt = sqliteDb.prepare(`
      INSERT INTO players (uid, name, photoURL, wins, losses, lastSeen)
      VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ON CONFLICT(uid) DO UPDATE SET
        name = COALESCE(NULLIF(excluded.name, ''), players.name),
        photoURL = COALESCE(NULLIF(excluded.photoURL, ''), players.photoURL),
        wins = MAX(players.wins, excluded.wins),
        losses = MAX(players.losses, excluded.losses),
        lastSeen = CURRENT_TIMESTAMP
    `);
    stmt.run(uid, name || 'Player', photoURL || '', wins || 0, losses || 0);
  } catch (e) {}
}

function sqliteRecordMatch(id, winner) {
  if (!sqliteDb || !id) return;
  try {
    const stmt = sqliteDb.prepare('INSERT OR REPLACE INTO matches (id, winner) VALUES (?, ?)');
    stmt.run(id, winner || 'Unknown');
  } catch (e) {}
}

function sqliteLogAudit(event, details) {
  if (!sqliteDb) return;
  try {
    const stmt = sqliteDb.prepare('INSERT INTO audit_logs (event, details) VALUES (?, ?)');
    stmt.run(event, typeof details === 'string' ? details : JSON.stringify(details));
  } catch (e) {}
}

// Startup Firestore Hydration / Background Sync
async function syncHistoricalDataFromFirestore() {
  if (!sqliteDb || typeof db === 'undefined' || !db) return;
  
  // 1. Sync Past Players
  try {
    const usersSnapshot = await db.collection('users').get();
    let userCount = 0;
    usersSnapshot.forEach(doc => {
      const data = doc.data();
      sqliteUpsertPlayer(doc.id, data.displayName || 'Player', data.photoURL || '', data.wins || 0, data.losses || 0);
      userCount++;
    });
    console.log(`✅ Synced ${userCount} past players from Firestore to local SQLite!`);
  } catch (err) {
    console.warn('⚠️ Firestore player sync notice:', err.message || err);
  }

  // 2. Sync Past Matches
  try {
    const matchesSnapshot = await db.collection('matches').orderBy('playedAt', 'desc').limit(100).get();
    let matchCount = 0;
    matchesSnapshot.forEach(doc => {
      const data = doc.data();
      const playedAtStr = data.playedAt ? (data.playedAt.toDate ? data.playedAt.toDate().toISOString() : data.playedAt) : new Date().toISOString();
      sqliteRecordMatch(doc.id, data.winnerUid || data.winner || 'Unknown');
      matchCount++;
    });
    console.log(`✅ Synced ${matchCount} past matches from Firestore to local SQLite!`);
  } catch (err) {
    console.warn('⚠️ Firestore match sync notice (Quota active or offline): Using existing SQLite match records.');
  }
}

// Trigger background sync 5s after startup
setTimeout(() => {
  syncHistoricalDataFromFirestore();
}, 5000);
// ============================================================================


function makeQuotaSafeHandler(handler, fallbackPayload = {}) {
  return async (req, res, next) => {
    try {
      await handler(req, res, next);
    } catch (err) {
      console.warn('Quota/route intercept on', req.originalUrl, ':', err && err.message ? err.message : err);
      return res.status(200).json(Object.assign({
        degraded: true,
        error: 'Firestore quota limit reached. System running in degraded mode.',
        players: [],
        matches: [],
        auditLogs: [],
        securityLogs: [],
        metrics: { activeMatches: 0, connectedPlayers: 0 }
      }, fallbackPayload));
    }
  };
}


// QUOTA_SAFE_PATCH_APPLIED 2026-10-07T19:55:22.490Z

process.on('unhandledRejection', (reason, promise) => {
  if (reason && (reason.code === 8 || reason.message?.includes('RESOURCE_EXHAUSTED'))) {
    console.warn('⚠️ Intercepted async Firestore quota exhaustion promise rejection.');
    return;
  }
  console.error('Unhandled Rejection at:', promise, 'reason:', reason);
});


// Safe wrapper for async Firestore admin routes
const safeAdminHandler = (handler, fallbackData = {}) => async (req, res, next) => {
  try {
    await handler(req, res, next);
  } catch (err) {
    if (err && (err.code === 8 || err.message?.includes('RESOURCE_EXHAUSTED') || err.message?.includes('Quota exceeded'))) {
      console.warn(`⚠️ Firestore quota hit on ${req.path} - returning degraded fallback payload.`);
      return res.status(200).json({
        degraded: true,
        error: 'Firestore quota limit exceeded. Operating in degraded mode.',
        ...fallbackData
      });
    }
    console.error(`❌ Unhandled error on ${req.path}:`, err);
    return res.status(200).json({ degraded: true, ...fallbackData });
  }
};

const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const crypto = require('crypto');

// ─── Config ───
const PORT = process.env.PORT || 3001;
const MAX_PLAYERS_PER_ROOM = 6;
const FOURTEEN_MINUTES_MS = 14 * 60 * 1000;

// ─── Firebase Admin Init ───
let firebaseReady = false;
let db = null;
let auth = null;
let FieldValue = null;

try {
    let serviceAccount;

    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        // Production (Render / Railway / etc.)
        serviceAccount = typeof process.env.FIREBASE_SERVICE_ACCOUNT === 'string'
            ? JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT)
            : process.env.FIREBASE_SERVICE_ACCOUNT;
    } else {
        // Local development
        serviceAccount = require('./serviceAccountKey.json');
    }

    // Modern v11/v12 Firebase Admin SDK imports (Node v20/v22+ compatible)
    const { initializeApp, cert } = require('firebase-admin/app');
    const { getFirestore, FieldValue: fv } = require('firebase-admin/firestore');
    const { getAuth } = require('firebase-admin/auth');

    initializeApp({
        credential: cert(serviceAccount),
    });

    db = getFirestore();
    auth = getAuth();
    FieldValue = fv;

    firebaseReady = true;
    console.log('🔥 Firebase Admin SDK initialized successfully!');
} catch (err) {
    console.warn('⚠️ Firebase Admin not initialized:', err.message);
    console.warn('   Server will still run, but token verification & Firestore writes are disabled.');
}

// ─── Express + Socket.io ───
const app = express();
app.use(cors({
    origin: true,
    methods: ['GET', 'POST', 'PATCH', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Authorization', 'Content-Type', 'x-admin-key', 'x-admin-token'],
}));
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*',
        methods: ['GET', 'POST'],
    },
});

// ─── Live Server State (RAM) ───
const queue = [];
const activeGames = {};
/** In-memory tournament bracket state; durable tournament storage is not configured. */
const tournaments = new Map();
const securityLogs = [];
const MAX_SECURITY_LOGS = 100;
let latestAnnouncement = null;
const latestNotices = [];
const activeConnections = new Map();
const recentConnections = [];
const ghostSpectators = new Map();
const adminSocketSessions = new Map();
const trafficSimulations = new Map();
const telemetryHistory = [];
const gameplayActionTimes = [];
const metrics = {
    gamesStarted: 0,
    gamesCompleted: 0,
    forfeits: 0,
    actionErrors: 0,
    voiceEventsRelayed: 0,
    cardsPlayed: 0,
    lastGameAt: null,
};
let matchmakingPaused = false;
let happyHour = { enabled: false, multiplier: 2, startsAt: null, endsAt: null, label: 'Happy Hour' };
let featureFlags = {
    maintenanceMode: false,
    voiceEnabled: true,
    rankedEnabled: true,
    maxPlayers: MAX_PLAYERS_PER_ROOM,
    minStartingCards: 4,
    maxStartingCards: 8,
    autoFillQueueWithBots: false,
};

// ─── Helpers ───
function log(emoji, message) {
    console.log(`[${new Date().toLocaleTimeString()}] ${emoji} ${message}`);
}

function isNonEmptyString(value, maxLength = 100) {
    return typeof value === 'string' && value.trim().length > 0 && value.length <= maxLength;
}

function recordSecurityEvent(type, severity, { uid = null, name = null, roomId = null, ip = null, details = '' } = {}) {
    const event = {
        id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
        timestamp: new Date().toISOString(),
        type,
        uid,
        name,
        roomId,
        severity,
        details: String(details).slice(0, 300),
    };
    securityLogs.unshift(event);
    if (securityLogs.length > MAX_SECURITY_LOGS) securityLogs.length = MAX_SECURITY_LOGS;
    log('SECURITY', `${severity} ${type} uid=${uid || 'unknown'} room=${roomId || 'none'} ${event.details}`);
    return event;
}

function announcementIsActive(announcement) {
    return Boolean(announcement && (!announcement.expiresAt || new Date(announcement.expiresAt).getTime() > Date.now()));
}

function pruneNotices() {
    const now = Date.now();
    for (let index = latestNotices.length - 1; index >= 0; index -= 1) {
        if (latestNotices[index].expiresAt && new Date(latestNotices[index].expiresAt).getTime() <= now) {
            latestNotices.splice(index, 1);
        }
    }
    if (latestNotices.length > 20) latestNotices.splice(0, latestNotices.length - 20);
    latestAnnouncement = latestNotices[latestNotices.length - 1] || null;
    return latestNotices;
}

function createAnnouncement({ id, message, priority, sticky, expiresAt } = {}) {
    const cleanMessage = String(message || '').trim();
    if (!isNonEmptyString(cleanMessage, 500)) throw new Error('Announcement must be 1–500 characters.');
    const cleanPriority = ['info', 'warning', 'urgent'].includes(priority) ? priority : 'info';
    let normalizedExpiry = null;
    if (expiresAt) {
        const parsedExpiry = new Date(expiresAt);
        if (!Number.isFinite(parsedExpiry.getTime()) || parsedExpiry.getTime() <= Date.now()) {
            throw new Error('Announcement expiry must be a valid future date.');
        }
        normalizedExpiry = parsedExpiry.toISOString();
    }
    return {
        id: id || `announcement-${Date.now()}-${crypto.randomBytes(4).toString('hex')}`,
        message: cleanMessage,
        priority: cleanPriority,
        sticky: Boolean(sticky),
        createdAt: new Date().toISOString(),
        expiresAt: normalizedExpiry,
    };
}

function broadcastAnnouncement(payload) {
    const existingIndex = latestNotices.findIndex((notice) => notice.id === payload.id);
    if (existingIndex >= 0) latestNotices.splice(existingIndex, 1);
    latestNotices.push(payload);
    pruneNotices();
    io.emit('admin_broadcast', payload);
    io.emit('server_announcements', [...latestNotices]);
    return payload;
}

function removeAnnouncement(id) {
    const index = latestNotices.findIndex((notice) => notice.id === id);
    if (index < 0) return false;
    latestNotices.splice(index, 1);
    pruneNotices();
    io.emit('announcement_cleared', { id });
    io.emit('server_announcements', [...latestNotices]);
    return true;
}

function clientIp(socket) {
    const forwarded = process.env.TRUST_PROXY_GEO_HEADERS === 'true'
        ? socket.handshake?.headers?.['x-forwarded-for']
        : null;
    const ip = Array.isArray(forwarded) ? forwarded[0] : String(forwarded || '').split(',')[0].trim();
    return String(ip || socket.handshake?.address || socket.conn?.remoteAddress || 'unknown').replace(/^::ffff:/, '');
}

function coarseGeo(socket) {
    if (process.env.TRUST_PROXY_GEO_HEADERS !== 'true') return { city: 'Unknown', country: 'Unknown' };
    const headers = socket.handshake?.headers || {};
    const readHeader = (...names) => {
        for (const name of names) {
            const raw = headers[name];
            const value = Array.isArray(raw) ? raw[0] : raw;
            if (typeof value === 'string' && value.trim()) {
                try { return decodeURIComponent(value.trim()).slice(0, 100); } catch { return value.trim().slice(0, 100); }
            }
        }
        return '';
    };
    return {
        city: readHeader('x-vercel-ip-city', 'cf-ipcity') || 'Unknown',
        country: readHeader('x-vercel-ip-country', 'cf-ipcountry', 'cloudfront-viewer-country') || 'Unknown',
    };
}

function trackConnection(socket, identity = {}) {
    const previous = activeConnections.get(socket.id) || {};
    const location = coarseGeo(socket);
    const record = {
        socketId: socket.id,
        uid: identity.uid || previous.uid || null,
        name: identity.name || previous.name || null,
        ip: clientIp(socket),
        city: location.city,
        country: location.country,
        region: location.city !== 'Unknown' && location.country !== 'Unknown'
            ? `${location.city}, ${location.country}`
            : location.country !== 'Unknown' ? location.country : 'Unknown',
        connectedAt: previous.connectedAt || new Date().toISOString(),
    };
    activeConnections.set(socket.id, record);
    const recentIndex = recentConnections.findIndex((connection) => connection.socketId === socket.id);
    if (recentIndex >= 0) recentConnections.splice(recentIndex, 1);
    recentConnections.unshift({ ...record });
    if (recentConnections.length > 100) recentConnections.length = 100;
    return record;
}

function publicConnection(record) {
    return {
        socketId: record.socketId,
        uid: record.uid,
        name: record.name,
        city: record.city,
        country: record.country,
        region: record.region,
        connectedAt: record.connectedAt,
    };
}

function aggregateGeo() {
    const countries = new Map();
    const cities = new Map();
    let unknownCount = 0;
    for (const connection of activeConnections.values()) {
        const country = connection.country || 'Unknown';
        if (country === 'Unknown') unknownCount += 1;
        countries.set(country, (countries.get(country) || 0) + 1);
        const city = connection.city && connection.city !== 'Unknown'
            ? `${connection.city}, ${country}`
            : country;
        cities.set(city, (cities.get(city) || 0) + 1);
    }

    return {
        connectionsByCountry: [...countries].map(([country, count]) => ({ country, count })).sort((a, b) => b.count - a.count),
        activePoints: [...cities].map(([region, count]) => ({ city: region, count })).sort((a, b) => b.count - a.count),
        unknownCount,
        recentConnections: recentConnections.map(publicConnection),
    };
}

function happyHourIsActive() {
    if (!happyHour.enabled) return false;
    if (happyHour.endsAt && new Date(happyHour.endsAt).getTime() <= Date.now()) {
        happyHour = { ...happyHour, enabled: false };
        io.emit('happy_hour_updated', happyHour);
    }
    return happyHour.enabled;
}

function adminGameSnapshot(game) {
    return {
        ...gameSnapshot(game),
        handsByUid: Object.fromEntries(game.players.map((player) => [player.uid, game.handsByUid[player.uid] || []])),
        market: game.market,
    };
}

function broadcastGhostSnapshots(game) {
    const spectators = ghostSpectators.get(game.roomId);
    if (!spectators) return;
    const snapshot = adminGameSnapshot(game);
    for (const socketId of spectators) io.to(socketId).emit('admin_ghost_snapshot', snapshot);
}

async function authenticateAdminSocket(socket, token) {
    const admin = await requireAdminToken(token);
    adminSocketSessions.set(socket.id, {
        uid: admin.uid,
        email: admin.email,
        expiresAt: Date.now() + 5 * 60_000,
    });
    return admin;
}

function emitAdminPacket(packet) {
    for (const [socketId, session] of adminSocketSessions) {
        if (session.expiresAt <= Date.now() || !io.sockets.sockets.has(socketId)) {
            adminSocketSessions.delete(socketId);
            continue;
        }
        io.to(socketId).emit('admin_packet_stream', packet);
    }
}

function recordGameplayAction(socket, data, actionType, details = {}) {
    const actor = socket.playerData || {};
    const timestamp = Date.now();
    if (actionType === 'PLAY_CARD') gameplayActionTimes.push(timestamp);
    while (gameplayActionTimes.length && gameplayActionTimes[0] < timestamp - 60_000) gameplayActionTimes.shift();
    const sentAt = Number(data?.clientSentAt);
    const latencyMs = Number.isFinite(sentAt) && Math.abs(timestamp - sentAt) <= 60_000
        ? Math.abs(timestamp - sentAt)
        : null;
    emitAdminPacket({
        id: `${timestamp}-${Math.random().toString(36).slice(2, 8)}`,
        timestamp: new Date(timestamp).toISOString(),
        actionType,
        roomId: String(data?.roomId || ''),
        actorUid: actor.uid || null,
        actorName: actor.name || 'Unknown',
        latencyMs,
        ...details,
    });
}

function telemetrySnapshot() {
    const timestamp = Date.now();
    for (const [id, simulation] of trafficSimulations) {
        if (simulation.expiresAt <= timestamp) {
            clearInterval(simulation.timer);
            simulation.virtualSockets.forEach((mockSocket) => { mockSocket.connected = false; });
            trafficSimulations.delete(id);
        }
    }
    while (gameplayActionTimes.length && gameplayActionTimes[0] < timestamp - 60_000) gameplayActionTimes.shift();
    const simulatedPlayers = [...trafficSimulations.values()].reduce((sum, session) =>
        sum + session.virtualSockets.filter((mockSocket) => mockSocket.connected).length, 0);
    const simulatedRooms = [...trafficSimulations.values()].reduce((sum, session) => sum + session.rooms, 0);
    return {
        timestamp,
        concurrentPlayers: [...activeConnections.values()].filter((connection) => connection.uid).length + simulatedPlayers,
        activeRooms: Object.keys(activeGames).length + simulatedRooms,
        cardsPlayedPerMinute: gameplayActionTimes.filter((time) => time >= timestamp - 60_000 && time).length,
        simulatedPlayers,
    };
}

function sampleTelemetry() {
    const sample = telemetrySnapshot();
    telemetryHistory.push(sample);
    const cutoff = sample.timestamp - 15 * 60_000;
    while (telemetryHistory.length && telemetryHistory[0].timestamp < cutoff) telemetryHistory.shift();
}

setInterval(sampleTelemetry, 5000).unref?.();
sampleTelemetry();
setInterval(() => {
    if (!featureFlags.autoFillQueueWithBots) return;
    for (const entry of [...queue]) {
        if (Date.now() - Date.parse(entry.joinedAt) >= 30_000) void startBotFilledMatch(entry);
    }
}, 1000).unref?.();

function getServerStats() {
    return {
        activeRooms: Object.keys(activeGames).length,
        playersInRooms: Object.values(activeGames).reduce((sum, g) => sum + g.players.length, 0),
        queueSize: queue.length,
        uptimeSeconds: Math.floor(process.uptime()),
        firebaseReady,
    };
}

function publicRoomSummary(game) {
    return {
        roomId: game.roomId,
        state: game.state,
        hostUid: game.hostUid,
        hostName: game.players.find((player) => player.uid === game.hostUid)?.name || '',
        players: game.players.map((player) => ({
            uid: player.uid,
            name: player.name,
            photoURL: player.photoURL || null,
            isHost: player.uid === game.hostUid,
            isBot: Boolean(player.isBot),
        })),
        playerCount: game.players.length,
        settings: game.settings,
        turnIndex: game.turnIndex ?? 0,
        adminPaused: Boolean(game.adminPaused),
        createdAt: game.createdAt,
        matchId: game.matchId || null,
    };
}

function auditAdminAction(actor, action, target, meta = {}) {
    if (!db || !FieldValue) return Promise.resolve();
    return db.collection('adminLogs').add({
        actorEmail: actor.email,
        actorUid: actor.uid,
        action,
        target: String(target || ''),
        timestamp: FieldValue.serverTimestamp(),
        meta,
    }).catch((error) => {
        log('⚠️', `Admin audit write failed (${action}): ${error.message}`);
    });
}

function evaluateReplayAction(game, event) {
    const uid = event.actorUid;
    const context = event.analysisContext || {};
    if (!uid) return {};
    const hand = Array.isArray(context.actorHandBefore) ? context.actorHandBefore : [];
    const history = Array.isArray(context.discardPileBefore) ? context.discardPileBefore : [];
    const handCounts = context.handsCountBefore || {};
    const activeShape = String(context.activeShapeBefore || cardShape(history[history.length - 1]) || '').toLowerCase();
    const shapeTotals = Object.fromEntries(
        Object.entries({ circles: 11, triangles: 11, crosses: 10, squares: 10, stars: 7 })
            .map(([shape, count]) => [shape, count * Math.max(1, Number(game.settings?.decks) || 1)]),
    );
    const normalizeShape = (card) => String(cardShape(card) || '').toLowerCase();
    const discarded = Object.fromEntries(Object.keys(shapeTotals).map((shape) => [shape, 0]));
    for (const card of history) {
        const shape = normalizeShape(card);
        if (Object.hasOwn(discarded, shape)) discarded[shape] += 1;
    }
    const opponents = Object.entries(handCounts).filter(([opponentUid]) => opponentUid !== uid);
    const shapeCall = event.actionType === 'call_whot' && event.requestedShape;
    let grade;
    let insight;

    if (event.actionType === 'draw' && activeShape
        && !hand.some((card) => normalizeShape(card) === activeShape)) {
        const gaps = game.analysisKnownGaps || (game.analysisKnownGaps = {});
        const playerGaps = gaps[uid] || (gaps[uid] = []);
        if (!playerGaps.includes(activeShape)) playerGaps.push(activeShape);
    }

    const requestedShape = String(event.requestedShape || '').toLowerCase();
    const exploitedGap = requestedShape && opponents.some(([opponentUid]) =>
        game.analysisKnownGaps?.[opponentUid]?.includes(requestedShape),
    );
    if (shapeCall) {
        const chosen = requestedShape;
        const total = shapeTotals[chosen];
        if (exploitedGap) {
            grade = 'great';
            insight = `Called ${chosen} to exploit a previously observed opponent gap.`;
        } else if (total) {
            const ratio = (discarded[chosen] || 0) / total;
            const bestShape = Object.keys(shapeTotals).reduce((best, shape) => {
                const remaining = (shapeTotals[shape] - discarded[shape]) / shapeTotals[shape];
                return remaining > best.remaining ? { shape, remaining } : best;
            }, { shape: '', remaining: -1 });
            if (ratio >= 0.75 && bestShape.shape !== chosen && bestShape.remaining > 1 - ratio) {
                grade = 'inaccuracy';
                insight = `Called ${chosen} after ${Math.round(ratio * 100)}% of its cards were discarded.`;
            } else {
                grade = 'best';
                insight = `Called ${chosen} with ${Math.round((1 - ratio) * 100)}% of its deck cards still unseen.`;
            }
        }
    } else if (event.actionType === 'play' || event.actionType === 'whot') {
        const card = event.card;
        const number = cardNumber(card);
        const shape = normalizeShape(card);
        const threats = opponents.filter(([, count]) => Number(count) <= 2);
        const isWild = isWildCard(card);
        const top = history[history.length - 1];
        const playableNonWild = hand.some((candidate) => {
            if (isWildCard(candidate)) return false;
            const candidateShape = normalizeShape(candidate);
            return activeShape
                ? candidateShape === activeShape || cardNumber(candidate) === cardNumber(top)
                : candidateShape === normalizeShape(top) || cardNumber(candidate) === cardNumber(top);
        });
        const pickCard = number === 2 || number === 5;
        if (pickCard && threats.length) {
            grade = 'great';
            insight = `Applied a pick penalty with an opponent at ${Math.min(...threats.map(([, count]) => Number(count)))} cards.`;
        } else if (isWild && playableNonWild) {
            grade = 'blunder';
            insight = 'Used a Whot wildcard while a non-wild playable card was available.';
        } else if (shape && opponents.some(([opponentUid]) =>
            game.analysisKnownGaps?.[opponentUid]?.includes(shape),
        )) {
            grade = 'great';
            insight = `Played ${shape} against a previously observed opponent gap.`;
        } else {
            const playable = hand.filter((candidate) => {
                if (isWildCard(candidate)) return true;
                const candidateShape = normalizeShape(candidate);
                return activeShape
                    ? candidateShape === activeShape || cardNumber(candidate) === cardNumber(top)
                    : candidateShape === normalizeShape(top) || cardNumber(candidate) === cardNumber(top);
            });
            const bestShape = playable.reduce((best, candidate) => {
                const candidateShape = normalizeShape(candidate);
                const candidateTotal = shapeTotals[candidateShape] || 1;
                const remaining = (candidateTotal - (discarded[candidateShape] || 0)) / candidateTotal;
                return remaining > best.remaining ? { shape: candidateShape, remaining } : best;
            }, { shape: '', remaining: -1 });
            const remainingRatio = shapeTotals[shape]
                ? (shapeTotals[shape] - (discarded[shape] || 0)) / shapeTotals[shape]
                : 0;
            if (bestShape.shape && bestShape.shape !== shape && remainingRatio < 0.25) {
                grade = 'inaccuracy';
                insight = `Played ${shape} with few cards remaining despite another legal option.`;
            } else {
                grade = 'best';
                insight = 'Selected a strong legal play from the information available.';
            }
        }
    }

    const serial = context.turnSerialBefore;
    if (event.actionType === 'play' && [2, 5].includes(cardNumber(event.card))) {
        game.analysisHeldPickTurns = game.analysisHeldPickTurns || {};
        game.analysisHeldPickTurns[uid] = 0;
    } else if (serial !== undefined && ['play', 'draw', 'pass', 'penalty_draw'].includes(event.actionType)) {
        game.analysisLastTurnSerial = game.analysisLastTurnSerial || {};
        if (game.analysisLastTurnSerial[uid] !== serial) {
            game.analysisLastTurnSerial[uid] = serial;
            const holdingPick = hand.some((card) => [2, 5].includes(cardNumber(card)));
            game.analysisHeldPickTurns = game.analysisHeldPickTurns || {};
            game.analysisHeldPickTurns[uid] = holdingPick ? (game.analysisHeldPickTurns[uid] || 0) + 1 : 0;
            if (game.analysisHeldPickTurns[uid] >= 4 && opponents.some(([, count]) => Number(count) <= 2)) {
                grade = 'blunder';
                insight = `Held a pick penalty for ${game.analysisHeldPickTurns[uid]} turns while an opponent had two or fewer cards.`;
            }
        }
    }

    if (grade) {
        game.analysisEvaluations = game.analysisEvaluations || [];
        game.analysisEvaluations.push({ uid, grade, insight, turnNumber: game.replayTurnNumber || 0 });
    }
    return { grade, insight };
}

function replaySnapshot(game, event) {
    const top = game.discardPile?.[game.discardPile.length - 1] || null;
    const analysis = event.analysisContext || {};
    const evaluation = evaluateReplayAction(game, event);
    const toReplayCard = (card) => ({ id: card.id, shape: cardShape(card), number: cardNumber(card) });
    const matchPlayers = game.matchPlayers || game.players;
    return {
        turnNumber: game.replayTurnNumber || 0,
        actorUid: event.actorUid || null,
        actorName: event.actorName || null,
        actionType: event.actionType,
        card: event.card ? {
            id: event.card.id,
            shape: cardShape(event.card),
            number: cardNumber(event.card),
        } : null,
        requestedShape: event.requestedShape || null,
        penaltyAmount: event.penaltyAmount ?? null,
        turnIndexBefore: event.turnIndexBefore ?? game.turnIndex,
        turnIndexAfter: game.turnIndex,
        turnSerialBefore: analysis.turnSerialBefore ?? game.turnSerial ?? 0,
        turnSerialAfter: game.turnSerial ?? 0,
        marketCountAfter: game.market?.length ?? 0,
        marketCountBefore: analysis.marketCountBefore ?? game.market?.length ?? 0,
        discardTopAfter: top ? {
            id: top.id,
            shape: cardShape(top),
            number: cardNumber(top),
        } : null,
        discardPileAfter: (game.discardPile || []).map(toReplayCard),
        discardPileBefore: (analysis.discardPileBefore || []).map(toReplayCard),
        handsCountBefore: analysis.handsCountBefore || {},
        activeShapeBefore: analysis.activeShapeBefore || null,
        activeShapeAfter: game.activeSuit || null,
        handsCountAfter: Object.fromEntries(matchPlayers.map((player) => [
            player.uid,
            (game.handsByUid?.[player.uid] || []).length,
        ])),
        ...evaluation,
        timestamp: new Date().toISOString(),
    };
}

function captureReplayContext(game, actorUid) {
    return {
        turnSerialBefore: game.turnSerial || 0,
        marketCountBefore: game.market?.length || 0,
        activeShapeBefore: game.activeSuit || null,
        actorHandBefore: [...(game.handsByUid?.[actorUid] || [])],
        discardPileBefore: [...(game.discardPile || [])],
        handsCountBefore: Object.fromEntries((game.matchPlayers || game.players).map((player) => [
            player.uid,
            (game.handsByUid?.[player.uid] || []).length,
        ])),
    };
}

function recordReplayTurn(game, event) {
    if (!db || !game.matchId) return Promise.resolve();
    game.replayTurnNumber = (game.replayTurnNumber || 0) + 1;
    const turn = replaySnapshot(game, event);
    game.replayWrite = (game.replayWrite || Promise.resolve()).then(async () => {
        await db.collection('matches').doc(game.matchId)
            .collection('turns').doc(String(turn.turnNumber).padStart(6, '0')).set(turn);
        await db.collection('matches').doc(game.matchId).set({
            totalMoves: turn.turnNumber,
            lastActionAt: turn.timestamp,
        }, { merge: true });
    }).catch((error) => {
        log('⚠️', `Replay turn ${turn.turnNumber} write failed: ${error.message}`);
    });
    return game.replayWrite;
}

async function createReplayMatch(game) {
    if (!db) return;
    const matchRef = db.collection('matches').doc();
    game.matchId = matchRef.id;
    game.startedAt = new Date();
    game.replayTurnNumber = -1;
    const players = game.players.map((player, seatIndex) => ({
        uid: player.uid,
        name: player.name,
        ...(player.photoURL ? { photoURL: player.photoURL } : {}),
        ...(player.isBot ? { isBot: true } : {}),
        seatIndex,
    }));
    game.matchPlayers = players;
    await matchRef.set({
        roomId: game.roomId,
        mode: 'online',
        players,
        participantUids: players.map((player) => player.uid),
        winnerUid: null,
        starterUid: game.players[game.turnIndex]?.uid || null,
        settings: {
            startingCards: game.settings.startingHandSize,
            decks: game.settings.decks,
        },
        ranked: game.ranked !== false,
        createdAt: FieldValue.serverTimestamp(),
        startedAt: FieldValue.serverTimestamp(),
        status: 'playing',
        totalMoves: 0,
    });
    await recordReplayTurn(game, {
        actionType: 'game_start',
        actorUid: null,
        actorName: null,
        turnIndexBefore: game.turnIndex,
    });
}

async function finalizeReplayMatch(game, { winnerUid = null, resultType = 'normal' } = {}) {
    if (!db || !game.matchId) return;
    if (game.replayFinalized) return game.replayFinalization || Promise.resolve();
    game.replayFinalized = true;
    game.replayFinalization = (async () => {
        await recordReplayTurn(game, {
            actionType: 'game_end',
            actorUid: winnerUid,
            actorName: (game.matchPlayers || game.players).find((player) => player.uid === winnerUid)?.name || null,
            turnIndexBefore: game.turnIndex,
        });
        await game.replayWrite;
        const endedAt = new Date();
        const analysisEvaluations = game.analysisEvaluations || [];
        const analysisReports = (game.matchPlayers || game.players).map((player) => {
            const evaluated = analysisEvaluations.filter((entry) => entry.uid === player.uid);
            const breakdown = { great: 0, best: 0, inaccuracy: 0, blunder: 0 };
            for (const entry of evaluated) breakdown[entry.grade] += 1;
            const optimal = breakdown.great + breakdown.best;
            return {
                uid: player.uid,
                name: player.name,
                accuracy: evaluated.length ? Math.round((optimal / evaluated.length) * 100) : 0,
                evaluatedMoves: evaluated.length,
                breakdown,
                insights: evaluated
                    .filter((entry) => entry.insight && ['great', 'inaccuracy', 'blunder'].includes(entry.grade))
                    .slice(0, 5)
                    .map((entry) => `Move ${entry.turnNumber}: ${entry.insight}`),
            };
        });
        await db.collection('matches').doc(game.matchId).set({
            winnerUid,
            endedAt: FieldValue.serverTimestamp(),
            durationSec: game.startedAt ? Math.max(0, Math.floor((endedAt - game.startedAt) / 1000)) : 0,
            totalMoves: Math.max(0, game.replayTurnNumber - 1),
            resultType,
            status: 'ended',
            finalSummary: {
                handCounts: Object.fromEntries((game.matchPlayers || game.players).map((player) => [
                    player.uid,
                    (game.handsByUid?.[player.uid] || []).length,
                ])),
                analysisReports,
            },
            analysisReports,
        }, { merge: true });
    })();
    return game.replayFinalization;
}

async function requireAdminToken(token) {
    if (!firebaseReady || !auth || !ADMIN_EMAIL) throw new Error('Admin authentication is unavailable. Configure Firebase Admin and ADMIN_EMAIL.');
    const decoded = await verifyPlayerToken(token);
    if (!decoded?.email || decoded.email.toLowerCase() !== ADMIN_EMAIL) {
        throw new Error('Unauthorized.');
    }
    return { uid: decoded.uid, email: decoded.email };
}

async function adminAuthMiddleware(req, res, next) {
    try {
        const authorization = String(req.headers.authorization || '');
        const keyParam = req.query.key || req.headers['x-admin-key'] || req.headers['x-admin-token'];
        const configuredAdminKey = String(process.env.ADMIN_KEY || '');
        if (configuredAdminKey && keyParam !== undefined) {
            const supplied = Buffer.from(String(keyParam));
            const configured = Buffer.from(configuredAdminKey);
            if (supplied.length === configured.length && crypto.timingSafeEqual(supplied, configured)) {
                req.admin = { uid: 'admin-key', email: 'admin-key' };
                req.adminAuthMethod = 'admin_key';
                return next();
            }
        }

        if (authorization.startsWith('Bearer ') && firebaseReady && auth) {
            const decoded = await auth.verifyIdToken(authorization.slice('Bearer '.length).trim());
            const userEmail = String(decoded?.email || '').trim().toLowerCase();
            if (userEmail && ADMIN_EMAIL && userEmail === ADMIN_EMAIL) {
                req.admin = { uid: decoded.uid, email: decoded.email };
                req.adminUser = decoded;
                req.adminAuthMethod = 'firebase_token';
                return next();
            }
        }

        return res.status(401).json({ error: 'Unauthorized admin access. Invalid key or token.' });
    } catch (error) {
        return res.status(401).json({ error: 'Unauthorized. Auth verification failed.' });
    }
}

const adminAuth = adminAuthMiddleware;

function emitActionError(socket, message, event = 'action_error') {
    metrics.actionErrors += 1;
    socket.emit(event, { error: message });
}

async function loadFeatureFlags() {
    if (!db) return;
    try {
        const snapshot = await db.collection('adminConfig').doc('flags').get();
        if (snapshot.exists) featureFlags = { ...featureFlags, ...snapshot.data() };
        const happyHourSnapshot = await db.collection('adminConfig').doc('happyHour').get();
        if (happyHourSnapshot.exists) {
            happyHour = { ...happyHour, ...happyHourSnapshot.data() };
            happyHourIsActive();
        }
    } catch (error) {
        log('⚠️', `Feature flag load failed: ${error.message}`);
    }
}

function emitRoomClosed(game, message) {
    io.to(game.roomId).emit('room_closed', { roomId: game.roomId, message });
    const spectators = ghostSpectators.get(game.roomId);
    if (spectators) {
        for (const socketId of spectators) io.to(socketId).emit('admin_ghost_snapshot', null);
        ghostSpectators.delete(game.roomId);
    }
    for (const player of game.players) {
        const connected = io.sockets.sockets.get(player.socketId);
        if (connected) {
            connected.leave(game.roomId);
            connected.data.roomId = null;
        }
    }
    delete activeGames[game.roomId];
}

async function enforcePlayerBan(uid) {
    const queued = queue.filter((entry) => entry.playerData.uid === uid);
    for (const entry of queued) {
        emitActionError(entry.socket, 'Your account is restricted from online play.');
    }
    for (let index = queue.length - 1; index >= 0; index -= 1) {
        if (queue[index].playerData.uid === uid) queue.splice(index, 1);
    }

    const affectedRooms = Object.values(activeGames).filter((game) =>
        game.players.some((player) => player.uid === uid),
    );
    for (const game of affectedRooms) {
        const player = game.players.find((entry) => entry.uid === uid);
        const targetSocket = player && io.sockets.sockets.get(player.socketId);
        if (targetSocket) {
            targetSocket.emit('room_closed', {
                roomId: game.roomId,
                message: 'Your account has been restricted from online play.',
            });
            targetSocket.leave(game.roomId);
            targetSocket.data.roomId = null;
        }
        game.players = game.players.filter((entry) => entry.uid !== uid);
        if (game.state === 'playing') {
            game.state = 'ended';
            game.status = 'ended';
            game.winnerUid = game.players.length === 1 ? game.players[0].uid : null;
            await persistAdminEndedGame(game, { winnerUid: game.winnerUid, resultType: 'player_ban' });
            io.to(game.roomId).emit('game_ended', {
                roomId: game.roomId,
                winnerUid: game.winnerUid,
                matchId: game.matchId || null,
                resultType: 'player_ban',
            });
            delete activeGames[game.roomId];
        } else if (game.players.length === 0) {
            delete activeGames[game.roomId];
        } else {
            if (game.hostUid === uid) game.hostUid = game.players[0].uid;
            syncHostState(game);
            broadcastRoom(game);
        }
    }
}

function progressionForMatch(current, won) {
    const wins = (typeof current.wins === 'number' ? current.wins : 0) + (won ? 1 : 0);
    const matches = (typeof current.matchesPlayed === 'number' ? current.matchesPlayed : 0) + 1;
    const streak = won ? (typeof current.winStreak === 'number' ? current.winStreak : 0) + 1 : 0;
    const unlocked = new Set(Array.isArray(current.achievementsUnlocked) ? current.achievementsUnlocked : []);
    if (wins >= 1) unlocked.add('first_win');
    if (wins >= 10) unlocked.add('wins_10');
    if (wins >= 25) unlocked.add('wins_25');
    if (matches >= 10) unlocked.add('table_regular');
    if (streak >= 3) unlocked.add('undefeated_3');
    return { xpEarned: won ? 150 : 50, achievementsUnlocked: [...unlocked] };
}

async function persistAdminEndedGame(game, { winnerUid = null, resultType = 'forfeit' } = {}) {
    game.state = 'ended';
    game.status = 'ended';
    game.winnerUid = winnerUid;
    game.ending = true;
    await finalizeReplayMatch(game, { winnerUid, resultType });
    metrics.gamesCompleted += 1;
    if (resultType !== 'normal') metrics.forfeits += 1;

    if (!db || !FieldValue || game.ranked === false || (!winnerUid && resultType !== 'normal')) return;
    const players = [...new Map((game.matchPlayers || game.players)
        .filter((player) => !player.isBot && player.uid).map((player) => [player.uid, player])).values()];
    const happyHourMultiplier = happyHourIsActive() ? happyHour.multiplier : 1;
    const userRefs = players.map((player) => db.collection('users').doc(player.uid));
    await db.runTransaction(async (transaction) => {
        const snapshots = await Promise.all(userRefs.map((ref) => transaction.get(ref)));
        players.forEach((player, index) => {
            const current = snapshots[index].exists ? snapshots[index].data() : {};
            const won = player.uid === winnerUid;
            const streak = typeof current.winStreak === 'number' ? current.winStreak : 0;
            const best = typeof current.bestWinStreak === 'number' ? current.bestWinStreak : 0;
            const progression = progressionForMatch(current, won);
            transaction.set(userRefs[index], {
                wins: FieldValue.increment(won ? 1 : 0),
                losses: FieldValue.increment(winnerUid && !won ? 1 : 0),
                matchesPlayed: FieldValue.increment(1),
                winStreak: won ? FieldValue.increment(1) : 0,
                bestWinStreak: won ? Math.max(best, streak + 1) : best,
                rankingPoints: FieldValue.increment(won ? happyHourMultiplier : 0),
                happyHourWins: FieldValue.increment(won && happyHourMultiplier > 1 ? 1 : 0),
                xp: FieldValue.increment(progression.xpEarned),
                achievementsUnlocked: progression.achievementsUnlocked,
                updatedAt: new Date().toISOString(),
                lastPlayedAt: new Date().toISOString(),
            }, { merge: true });
        });
    });
}

let cachedMatchMetrics = null;
let cachedMatchMetricsAt = 0;
const ADMIN_METRICS_CACHE_TTL = 15 * 60 * 1000;

async function adminMetrics() {
    const rooms = Object.values(activeGames).map(publicRoomSummary);
    let gamesLast24h = 0;
    let totalMatches = metrics.gamesCompleted;
    let forfeitCount = metrics.forfeits;
    let degraded = false;
    let cached = false;
    if (db) {
        try {
            const recentCutoff = new Date(Date.now() - 24 * 60 * 60 * 1000);
            const recentMatches = await db.collection('matches')
                .where('createdAt', '>=', recentCutoff)
                .get();
            gamesLast24h = recentMatches.size;
            const completedMatches = await db.collection('matches').where('status', '==', 'ended').get();
            totalMatches = completedMatches.size;
            forfeitCount = completedMatches.docs.filter((match) => {
                const resultType = match.data().resultType;
                return Boolean(resultType && resultType !== 'normal');
            }).length;
        } catch (error) {
            log('⚠️', `Admin match metrics query failed: ${error.message}`);
            degraded = true;
            if (cachedMatchMetrics && Date.now() - cachedMatchMetricsAt <= ADMIN_METRICS_CACHE_TTL) {
                ({ gamesLast24h, totalMatches, forfeitCount } = cachedMatchMetrics);
                cached = true;
            }
        }
    }
    if (db && !degraded) {
        cachedMatchMetrics = { gamesLast24h, totalMatches, forfeitCount };
        cachedMatchMetricsAt = Date.now();
    }
    const memory = process.memoryUsage();
    return {
        ...getServerStats(),
        ...metrics,
        totalConnections: io.engine.clientsCount,
        gamesLast24h,
        forfeitRate: totalMatches ? forfeitCount / totalMatches : 0,
        errorRate: metrics.actionErrors,
        memory: { rss: memory.rss, heapUsed: memory.heapUsed, heapTotal: memory.heapTotal },
        nodeEnv: process.env.NODE_ENV || 'development',
        socketHealthy: true,
        serverStatus: 'online',
        matchmakingPaused,
        flags: featureFlags,
        roomSummaries: rooms,
        degraded,
        cached,
    };
}

async function verifyPlayerToken(idToken) {
    if (!auth) return null;
    if (!isNonEmptyString(idToken, 2000)) {
        throw new Error('Missing or invalid Firebase ID token');
    }
    return auth.verifyIdToken(idToken);
}

function isPlayerBanned(user) {
    const hasBanExpiry = user.banUntil !== undefined && user.banUntil !== null;
    const banUntil = user.banUntil?.toDate ? user.banUntil.toDate() : new Date(user.banUntil);
    const expiryValid = hasBanExpiry && Number.isFinite(banUntil.getTime());
    const hasTimedBan = expiryValid && banUntil.getTime() > Date.now();
    const hasPermanentBan = (user.banned === true || user.isBanned === true || user.status === 'banned')
        && (!expiryValid || banUntil.getTime() > Date.now());
    return hasTimedBan || hasPermanentBan;
}

async function assertPlayerAllowed(uid) {
    if (!db || !uid) return;
    const snapshot = await db.collection('users').doc(uid).get();
    if (snapshot.exists && isPlayerBanned(snapshot.data())) throw new Error('Account suspended for policy violations.');
}

const DEFAULT_SETTINGS = { minPlayers: 2, maxPlayers: 6, startingHandSize: 6, decks: 1 };
const ADMIN_EMAIL = String(process.env.ADMIN_EMAIL || process.env.VITE_ADMIN_EMAIL || '').trim().toLowerCase();
const CARD_RANKS = {
    circles: [1, 2, 3, 4, 5, 7, 8, 10, 11, 12, 14],
    triangles: [1, 2, 3, 4, 5, 7, 8, 10, 11, 12, 14],
    crosses: [1, 2, 3, 5, 7, 8, 10, 11, 12, 14],
    squares: [1, 2, 3, 5, 7, 8, 10, 11, 12, 14],
    stars: [1, 2, 3, 4, 5, 7, 8],
};
const ACTION_LABELS = { 1: 'HOLD ON', 2: 'PICK TWO', 5: 'PICK THREE', 8: 'SUSPENSION', 14: 'GENERAL MARKET', 20: 'WHOT' };
function cardNumber(card) { return Number(card?.value ?? card?.number); }
function cardShape(card) { return String(card?.suit ?? card?.shape ?? '').toLowerCase(); }
function isWildCard(card) { return card?.isWild === true || cardNumber(card) === 20; }
function buildDeckPool(deckCount) {
    const deck = [];
    for (let copy = 0; copy < deckCount; copy += 1) {
        for (const [suit, ranks] of Object.entries(CARD_RANKS)) {
            ranks.forEach((value, index) => deck.push({
                id: `${copy}-${suit}-${value}-${index}`,
                suit, value, label: ACTION_LABELS[value] || String(value),
                isAction: [1, 2, 5, 8, 14].includes(value), isWild: false,
            }));
        }
        for (let index = 0; index < 5; index += 1) deck.push({
            id: `${copy}-whot-${index}`, suit: 'whot', value: 20, label: ACTION_LABELS[20], isAction: true, isWild: true,
        });
    }
    return deck;
}
function shuffleCards(cards) {
    for (let index = cards.length - 1; index > 0; index -= 1) {
        const otherIndex = Math.floor(Math.random() * (index + 1));
        [cards[index], cards[otherIndex]] = [cards[otherIndex], cards[index]];
    }
    return cards;
}
function createDeal(room) {
    const deck = shuffleCards(buildDeckPool(room.settings.decks));
    const handsByUid = {};
    for (const player of room.players) handsByUid[player.uid] = deck.splice(0, room.settings.startingHandSize);
    let openerIndex = deck.findIndex((card) => !card.isAction && !card.isWild);
    if (openerIndex < 0) openerIndex = 0;
    const discardTop = deck.splice(openerIndex, 1)[0];
    room.handsByUid = handsByUid;
    room.market = deck;
    room.discardPile = [discardTop];
    room.activeSuit = null;
    room.pendingPenalty = 0;
    room.penaltyType = null;
    room.hasDrawnThisTurn = false;
    room.playedCount = 0;
    room.status = 'playing';
    return { handsByUid, discardTop, marketCount: deck.length };
}
function syncHostState(room) {
    if (!room || !Array.isArray(room.players) || room.players.length === 0) return null;
    const nextHost = room.players.find((player) => player.uid === room.hostUid) || room.players[0];
    room.hostUid = nextHost ? nextHost.uid : room.hostUid;
    room.players.forEach((player) => { player.isHost = player.uid === room.hostUid; });
    return nextHost;
}
async function loadPlayerCosmetics(uid, fallbackPhotoURL = null) {
    let data = {};
    if (db) {
        try {
            const snapshot = await db.collection('users').doc(uid).get();
            if (snapshot.exists) data = snapshot.data() || {};
        } catch (error) {
            log('⚠️', `Could not load cosmetics for ${uid}: ${error.message}`);
        }
    }
    const allowed = (value, choices, fallback) => choices.includes(value) ? value : fallback;
    return {
        avatarId: allowed(data.avatarId, Array.from({ length: 12 }, (_, index) => `avatar_${String(index + 1).padStart(2, '0')}`), 'avatar_01'),
        avatarStyle: allowed(data.avatarStyle, ['preset', 'dicebear'], 'preset'),
        avatarFrameId: allowed(data.avatarFrameId, ['none', 'thin_steel', 'gold_ring', 'streak_ring', 'champion_laurel'], 'none'),
        titleId: allowed(data.titleId, ['the_seer', 'market_boss', 'last_card', 'sharp_player', 'senior_man', 'table_kingpin'], null),
        badgeId: allowed(data.badgeId, ['sharp', 'clutch', 'seer', 'market', 'chaos', 'patient', 'bully', 'diplomat'], null),
        cardBackId: allowed(data.cardBackId, ['classic', 'ankara', 'gold_foil', 'midnight_neon', 'naija_stripe', 'obsidian'], 'classic'),
        tableThemeId: allowed(data.tableThemeId, ['lagos_green', 'warri_emerald', 'royal_gold', 'night_black'], 'lagos_green'),
        showcaseBadgeIds: Array.isArray(data.showcaseBadgeIds) ? data.showcaseBadgeIds.filter((id) => typeof id === 'string').slice(0, 3) : [],
        photoURL: typeof data.photoURL === 'string' && (/^https:\/\/api\.dicebear\.com\/7\.x\/(bottts|identicon)\/svg\?seed=/.test(data.photoURL) || /^https:\/\/res\.cloudinary\.com\/[A-Za-z0-9_-]+\/image\/upload\//.test(data.photoURL)) ? data.photoURL : fallbackPhotoURL,
    };
}

function publicRoom(room) {
    syncHostState(room);
    return { roomId: room.roomId, hostUid: room.hostUid, players: room.players.map(({ uid, name, photoURL, avatarId, avatarStyle, avatarFrameId, titleId, badgeId, cardBackId, tableThemeId, showcaseBadgeIds, isHost, isAdmin, isBot, calledLastCard }) => ({ uid, name, photoURL, avatarId, avatarStyle, avatarFrameId, titleId, badgeId, cardBackId, tableThemeId, showcaseBadgeIds, isHost, isAdmin, isBot: Boolean(isBot), calledLastCard: Boolean(calledLastCard) })),
        state: room.state, settings: room.settings, maxPlayers: room.settings.maxPlayers };
}
function broadcastRoom(room) {
    syncHostState(room);
    io.to(room.roomId).emit('room_updated', publicRoom(room));
}
function suggestSettings(playerCount) {
    return { decks: playerCount >= 4 ? 2 : 1 };
}
function gameSnapshot(room) {
    return {
        roomId: room.roomId,
        players: publicRoom(room).players,
        handCounts: Object.fromEntries(room.players.map((player) => [player.uid, (room.handsByUid[player.uid] || []).length])),
        currentTurnUid: room.players[room.turnIndex]?.uid || '',
        currentTurnIndex: room.turnIndex,
        discardTop: room.discardPile[room.discardPile.length - 1],
        discardPile: room.discardPile,
        activeSuit: room.activeSuit,
        pendingPenalty: room.pendingPenalty,
        penaltyType: room.penaltyType,
        hasDrawnThisTurn: Boolean(room.hasDrawnThisTurn),
        marketCount: room.market.length,
        status: room.status || room.state,
        winnerUid: room.winnerUid,
        playedCount: room.playedCount,
        adminPaused: Boolean(room.adminPaused),
    };
}
function broadcastGameState(room) {
    io.to(room.roomId).emit('game_state', gameSnapshot(room));
    room.players.forEach((player) => {
        if (player.socketId) io.to(player.socketId).emit('hand_update', { roomId: room.roomId, hand: room.handsByUid[player.uid] || [] });
    });
    broadcastGhostSnapshots(room);
    scheduleBotTurn(room);
}
function recycleMarket(room) {
    if (room.market.length || room.discardPile.length <= 1) return;
    const topCard = room.discardPile.pop();
    room.market = shuffleCards(room.discardPile);
    room.discardPile = [topCard];
}
function drawFromMarket(room, uid, count = 1) {
    const hand = room.handsByUid[uid] || (room.handsByUid[uid] = []);
    for (let index = 0; index < count; index += 1) {
        recycleMarket(room);
        const card = room.market.pop();
        if (!card) break;
        hand.push(card);
    }
    return hand;
}

function scheduleBotTurn(room) {
    if (!room || room.state !== 'playing' || room.botTurnTimer || !room.players[room.turnIndex]?.isBot) return;
    room.botTurnTimer = setTimeout(async () => {
        room.botTurnTimer = null;
        if (room.state !== 'playing') return;
        const bot = room.players[room.turnIndex];
        if (!bot?.isBot) return;
        const hand = room.handsByUid[bot.uid] || [];
        const topCard = room.discardPile[room.discardPile.length - 1];
        const topValue = cardNumber(topCard);
        const activeShape = room.activeSuit ? String(room.activeSuit).toLowerCase() : null;
        const legalCards = hand.filter((card) => room.pendingPenalty > 0
            ? isWildCard(card) || (room.penaltyType === 'two' ? cardNumber(card) === 2 : cardNumber(card) === 5)
            : isWildCard(card) || (activeShape
                ? cardShape(card) === activeShape || cardNumber(card) === topValue
                : cardShape(card) === cardShape(topCard) || cardNumber(card) === topValue));
        const actorIndex = room.turnIndex;
        const analysisContext = captureReplayContext(room, bot.uid);
        if (legalCards.length) {
            legalCards.sort((left, right) => Number(isWildCard(left)) - Number(isWildCard(right))
                || Number([2, 5].includes(cardNumber(right))) - Number([2, 5].includes(cardNumber(left))));
            const card = legalCards[0];
            if (hand.length === 2 && !bot.calledLastCard) drawFromMarket(room, bot.uid, 1);
            hand.splice(hand.indexOf(card), 1);
            room.discardPile.push(card);
            room.playedCount += 1;
            metrics.cardsPlayed += 1;
            gameplayActionTimes.push(Date.now());
            room.hasDrawnThisTurn = false;
            const requestedShape = isWildCard(card)
                ? Object.entries(hand.filter((held) => cardShape(held) !== 'whot')
                    .reduce((counts, held) => ({ ...counts, [cardShape(held)]: (counts[cardShape(held)] || 0) + 1 }), {}))
                    .sort((a, b) => b[1] - a[1])[0]?.[0] || 'circles'
                : null;
            if (requestedShape) room.activeSuit = requestedShape;
            const next = (room.turnIndex + 1) % room.players.length;
            const value = cardNumber(card);
            if (hand.length === 0) {
                room.state = 'ended';
                room.status = 'ended';
                room.winnerUid = bot.uid;
            } else if (value === 2) {
                room.pendingPenalty += 2;
                room.penaltyType = 'two';
                room.turnIndex = next;
            } else if (value === 5) {
                room.pendingPenalty += 3;
                room.penaltyType = 'three';
                room.turnIndex = next;
            } else if (value === 1) {
                room.turnIndex = room.turnIndex;
            } else if (value === 8) {
                room.turnIndex = (room.turnIndex + 2) % room.players.length;
            } else if (value === 14) {
                room.players.filter((player) => player.uid !== bot.uid).forEach((player) => drawFromMarket(room, player.uid, 1));
                room.turnIndex = next;
            } else {
                if (!isWildCard(card)) room.activeSuit = null;
                room.turnIndex = next;
            }
            void recordReplayTurn(room, {
                actionType: isWildCard(card) ? 'whot' : 'play',
                actorUid: bot.uid,
                actorName: bot.name,
                card,
                requestedShape,
                turnIndexBefore: actorIndex,
                analysisContext,
            });
            emitAdminPacket({
                id: `${Date.now()}-bot`,
                timestamp: new Date().toISOString(),
                actionType: 'PLAY_CARD',
                roomId: room.roomId,
                actorUid: bot.uid,
                actorName: bot.name,
                latencyMs: 0,
                card: { label: card.label, value: card.value, suit: card.suit },
            });
            if (room.state === 'ended') io.to(room.roomId).emit('game_ended', { roomId: room.roomId, winnerUid: bot.uid });
            else io.to(room.roomId).emit('opponent_played_card', {
                roomId: room.roomId,
                playerId: bot.uid,
                senderUid: bot.uid,
                card,
                requestedShape,
                nextTurnIndex: room.turnIndex,
            });
        } else {
            const drawCount = room.pendingPenalty > 0 ? room.pendingPenalty : 1;
            drawFromMarket(room, bot.uid, drawCount);
            room.pendingPenalty = 0;
            room.penaltyType = null;
            room.hasDrawnThisTurn = false;
            room.turnIndex = (room.turnIndex + 1) % room.players.length;
            void recordReplayTurn(room, {
                actionType: 'draw',
                actorUid: bot.uid,
                actorName: bot.name,
                penaltyAmount: drawCount > 1 ? drawCount : null,
                turnIndexBefore: actorIndex,
                analysisContext,
            });
            io.to(room.roomId).emit('opponent_drew_card', { roomId: room.roomId, playerId: bot.uid, count: drawCount });
        }
        broadcastGameState(room);
    }, 900);
}

async function startBotFilledMatch(entry) {
    if (!featureFlags.autoFillQueueWithBots || !queue.includes(entry) || !entry.socket.connected) return;
    const queueIndex = queue.indexOf(entry);
    if (queueIndex < 0 || Date.now() - Date.parse(entry.joinedAt) < 30_000) return;
    queue.splice(queueIndex, 1);
    const botNames = ['Kofi', 'Amaka', 'Tunde'];
    const bot = {
        uid: `ai-bot-${crypto.randomBytes(6).toString('hex')}`,
        name: botNames[Math.floor(Math.random() * botNames.length)],
        socketId: null,
        isHost: false,
        isAdmin: false,
        isBot: true,
        ip: null,
    };
    const human = { socketId: entry.socket.id, ...entry.playerData, isHost: true, isBot: false };
    const roomId = `WHOT-AI-${crypto.randomBytes(3).toString('hex').toUpperCase()}`;
    const room = activeGames[roomId] = {
        roomId,
        hostUid: human.uid,
        players: [human, bot],
        state: 'playing',
        status: 'playing',
        settings: { ...DEFAULT_SETTINGS, ...suggestSettings(2), maxPlayers: 2 },
        turnIndex: 0,
        createdAt: new Date().toISOString(),
        ranked: featureFlags.rankedEnabled,
        turnSerial: 0,
        botFilled: true,
    };
    room.settings.startingHandSize = Math.max(featureFlags.minStartingCards, Math.min(6, featureFlags.maxStartingCards));
    entry.socket.join(roomId);
    entry.socket.data.roomId = roomId;
    const deal = createDeal(room);
    room.turnIndex = 0;
    if (deal.discardTop.value === 2) { room.pendingPenalty = 2; room.penaltyType = 'two'; }
    else if (deal.discardTop.value === 5) { room.pendingPenalty = 3; room.penaltyType = 'three'; }
    else if (deal.discardTop.value === 14) drawFromMarket(room, bot.uid, 1);
    metrics.gamesStarted += 1;
    metrics.lastGameAt = new Date().toISOString();
    try { await createReplayMatch(room); } catch (error) { log('⚠️', `Bot match replay initialization failed: ${error.message}`); }
    const payload = {
        roomId,
        matchId: room.matchId || null,
        players: publicRoom(room).players,
        settings: room.settings,
        startingPlayerIndex: room.turnIndex,
        startingPlayerUid: human.uid,
        startsFirst: true,
        discardTop: deal.discardTop,
        marketCount: deal.marketCount,
        gameState: gameSnapshot(room),
        botFilled: true,
    };
    entry.socket.emit('game_started', payload);
    entry.socket.emit('initial_hand', { roomId, hand: deal.handsByUid[human.uid] || [] });
    broadcastGameState(room);
    log('🤖', `Auto-filled queue entry ${human.uid} with ${bot.name} in ${roomId}.`);
}

// ─── Self-Ping Keep-Alive (Prevents Render Sleeping) ───
function startSelfPing() {
    const targetUrl = process.env.SERVER_URL || process.env.RENDER_EXTERNAL_URL;

    if (!targetUrl) {
        log('ℹ️', 'No RENDER_EXTERNAL_URL or SERVER_URL found. Self-ping skipped (normal for local dev).');
        return;
    }

    const healthEndpoint = `${targetUrl.replace(/\/$/, '')}/health`;
    log('⏰', `Self-ping keep-alive enabled! Target: ${healthEndpoint} (Every 14 minutes)`);

    setInterval(async () => {
        try {
            const response = await fetch(healthEndpoint);
            if (response.ok) {
                log('🏓', 'Self-ping successful. Server kept awake!');
            } else {
                log('⚠️', `Self-ping responded with status: ${response.status}`);
            }
        } catch (err) {
            log('⚠️', `Self-ping failed: ${err.message}`);
        }
    }, FOURTEEN_MINUTES_MS);
}

// ─── REST API Routes ───
app.get('/', (req, res) => {
    res.json({
        name: 'Whot Game Server',
        version: '1.4.0',
        status: 'online',
        firebaseReady,
        message: 'Whot backend is running. Connect via Socket.io for gameplay.',
    });
});

app.get('/health', (req, res) => {
    res.json({ status: 'ok', ...getServerStats() });
});

app.get('/api/announcement', (req, res) => {
    const notices = pruneNotices();
    res.json({
        announcement: announcementIsActive(latestAnnouncement) ? latestAnnouncement : null,
        announcements: notices,
        maintenanceMode: Boolean(featureFlags.maintenanceMode),
    });
});

app.get('/api/stats', (req, res) => {
    res.json(getServerStats());
});

app.get('/api/admin/metrics', adminAuth, async (req, res, next) => {
    try {
        res.json(await adminMetrics());
    } catch (error) {
        next(error);
    }
});

app.get('/api/admin/telemetry', adminAuth, (req, res) => {
    const snapshot = telemetrySnapshot();
    const history = [...telemetryHistory];
    if (!history.length || history[history.length - 1].timestamp !== snapshot.timestamp) history.push(snapshot);
    res.json({ current: snapshot, history: history.slice(-180) });
});

app.get('/api/admin/export', adminAuth, async (req, res, next) => {
    try {
        await auditAdminAction(req.admin, 'export_system_data', 'system');
        let auditLogs = [];
        if (db) {
            const auditSnapshot = await db.collection('adminLogs').orderBy('timestamp', 'desc').limit(1000).get();
            auditLogs = auditSnapshot.docs.map((entry) => ({ id: entry.id, ...entry.data() }));
        }
        const exportPayload = {
            generatedAt: new Date().toISOString(),
            rooms: Object.values(activeGames).map(publicRoomSummary),
            queue: queue.map((entry) => ({
                uid: entry.playerData.uid,
                name: entry.playerData.name,
                joinedAt: entry.joinedAt,
            })),
            auditLogs,
            stats: await adminMetrics(),
            telemetry: telemetrySnapshot(),
        };
        res.setHeader('Content-Disposition', 'attachment; filename="whot-system-backup.json"');
        res.json(exportPayload);
    } catch (error) {
        next(error);
    }
});

app.get('/api/admin/rooms', adminAuth, (req, res) => {
    res.json({ rooms: Object.values(activeGames).map(publicRoomSummary), queue: queue.map((entry) => ({
        uid: entry.playerData.uid,
        name: entry.playerData.name,
        joinedAt: entry.joinedAt,
    })) });
});

app.get('/api/admin/matches', adminAuth, async (req, res, next) => {
    try {
        if (!db) return res.status(503).json({ error: 'Match history is unavailable.' });
        const requested = Number(req.query.limit) || 50;
        const limit = Math.max(1, Math.min(100, requested));
        const snapshot = await db.collection('matches').orderBy('createdAt', 'desc').limit(limit).get();
        res.json({ matches: snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() })) });
    } catch (error) {
        next(error);
    }
});

app.get('/api/admin/matches/:matchId/replay', adminAuth, async (req, res, next) => {
    try {
        if (!db) return res.status(503).json({ error: 'Replay storage is unavailable.' });
        const matchRef = db.collection('matches').doc(String(req.params.matchId));
        const [matchSnapshot, turnsSnapshot] = await Promise.all([
            matchRef.get(),
            matchRef.collection('turns').orderBy('turnNumber', 'asc').limit(1000).get(),
        ]);
        if (!matchSnapshot.exists) return res.status(404).json({ error: 'Match not found.' });
        res.json({
            match: { id: matchSnapshot.id, ...matchSnapshot.data() },
            turns: turnsSnapshot.docs.map((turn) => turn.data()),
        });
    } catch (error) {
        next(error);
    }
});

app.get('/api/admin/players', adminAuth, async (req, res, next) => {
    try {
        if (!db) return res.status(503).json({ error: 'Player directory is unavailable.' });
        const search = String(req.query.q || '').trim().toLowerCase();
        const snapshot = await db.collection('users').orderBy('displayName').limit(500).get();
        const players = snapshot.docs.map((userDoc) => ({ uid: userDoc.id, ...userDoc.data() }))
            .filter((player) => !search || String(player.displayName || '').toLowerCase().includes(search)
                || String(player.email || '').toLowerCase().includes(search)
                || player.uid.toLowerCase().includes(search))
            .filter((player) => req.query.banned !== 'true' || isPlayerBanned(player))
            .map((player) => ({
                uid: player.uid,
                displayName: player.displayName || player.email || player.uid,
                email: player.email || '',
                banned: isPlayerBanned(player),
                matchesPlayed: player.matchesPlayed || 0,
                wins: player.wins || 0,
            }));
        res.json({ players });
    } catch (error) {
        next(error);
    }
});

app.get('/api/admin/security', adminAuth, async (req, res, next) => {
    try {
        const cutoff = Date.now() - 24 * 60 * 60 * 1000;
        const alerts24h = securityLogs.filter((event) => new Date(event.timestamp).getTime() >= cutoff).length;
        let activeBans = 0;
        if (db) {
            const users = await db.collection('users').limit(1000).get();
            activeBans = users.docs.filter((userDoc) => isPlayerBanned(userDoc.data())).length;
        }
        res.json({ securityLogs: [...securityLogs], alerts24h, activeBans });
    } catch (error) {
        next(error);
    }
});

app.get('/api/admin/geography', adminAuth, (req, res) => {
    res.json(aggregateGeo());
});

app.get('/api/admin/happy-hour', adminAuth, (req, res) => {
    happyHourIsActive();
    res.json({ happyHour });
});

app.put('/api/admin/happy-hour', adminAuth, async (req, res, next) => {
    try {
        const { enabled, multiplier, durationMinutes, label } = req.body || {};
        if (typeof enabled !== 'boolean' || !Number.isInteger(Number(multiplier))
            || Number(multiplier) < 1 || Number(multiplier) > 10
            || !Number.isInteger(Number(durationMinutes)) || Number(durationMinutes) < 1
            || Number(durationMinutes) > 1440) {
            return res.status(400).json({ error: 'Happy Hour requires enabled, a multiplier from 1 to 10, and a duration from 1 to 1440 minutes.' });
        }
        const now = Date.now();
        happyHour = {
            enabled,
            multiplier: Number(multiplier),
            startsAt: enabled ? new Date(now).toISOString() : null,
            endsAt: enabled ? new Date(now + Number(durationMinutes) * 60_000).toISOString() : null,
            label: String(label || 'Happy Hour').trim().slice(0, 60) || 'Happy Hour',
        };
        if (db) await db.collection('adminConfig').doc('happyHour').set(happyHour, { merge: true });
        io.emit('happy_hour_updated', happyHour);
        await auditAdminAction(req.admin, enabled ? 'start_happy_hour' : 'stop_happy_hour', 'global', happyHour);
        res.json({ happyHour });
    } catch (error) {
        next(error);
    }
});

app.patch('/api/admin/players/:uid/ban', adminAuth, async (req, res, next) => {
    try {
        if (!db) return res.status(503).json({ error: 'Player moderation is unavailable.' });
        const uid = String(req.params.uid || '');
        if (!uid || uid.length > 200 || typeof req.body?.banned !== 'boolean') {
            return res.status(400).json({ error: 'A valid player ID and banned boolean are required.' });
        }
        const durationHours = Number(req.body.durationHours);
        const banUntil = req.body.banned && Number.isFinite(durationHours) && durationHours > 0
            ? new Date(Date.now() + Math.min(8760, durationHours) * 60 * 60 * 1000)
            : null;
        await db.collection('users').doc(uid).set({
            banned: req.body.banned,
            status: req.body.banned ? 'banned' : 'active',
            banUntil,
            banReason: req.body.banned ? String(req.body.reason || 'Administrative moderation').slice(0, 300) : null,
            updatedAt: new Date().toISOString(),
        }, { merge: true });
        if (req.body.banned) {
            if (req.body.banned) await enforcePlayerBan(uid);
        }
        await auditAdminAction(req.admin, req.body.banned ? 'ban_player' : 'unban_player', uid, {
            durationHours: req.body.durationHours || null,
            reason: req.body.reason || null,
        });
        res.json({ success: true, uid, banned: req.body.banned, banUntil });
    } catch (error) {
        next(error);
    }
});

app.get('/api/admin/audit', adminAuth, async (req, res, next) => {
    try {
        if (!db) return res.status(503).json({ error: 'Audit history is unavailable.' });
        const snapshot = await db.collection('adminLogs').orderBy('timestamp', 'desc').limit(100).get();
        res.json({ entries: snapshot.docs.map((doc) => ({ id: doc.id, ...doc.data() })) });
    } catch (error) {
        next(error);
    }
});

app.get('/api/admin/flags', adminAuth, (req, res) => {
    res.json({ flags: featureFlags, matchmakingPaused });
});

app.patch('/api/admin/flags', adminAuth, async (req, res, next) => {
    try {
        const allowed = ['maintenanceMode', 'voiceEnabled', 'rankedEnabled', 'maxPlayers', 'minStartingCards', 'maxStartingCards', 'autoFillQueueWithBots'];
        const nextFlags = { ...featureFlags };
        for (const key of allowed) {
            if (Object.prototype.hasOwnProperty.call(req.body || {}, key)) nextFlags[key] = req.body[key];
        }
        if (typeof nextFlags.maintenanceMode !== 'boolean' || typeof nextFlags.voiceEnabled !== 'boolean'
            || typeof nextFlags.rankedEnabled !== 'boolean' || !Number.isInteger(nextFlags.maxPlayers)
            || nextFlags.maxPlayers < 2 || nextFlags.maxPlayers > MAX_PLAYERS_PER_ROOM
            || !Number.isInteger(nextFlags.minStartingCards) || nextFlags.minStartingCards < 4
            || !Number.isInteger(nextFlags.maxStartingCards) || nextFlags.maxStartingCards > 8
            || nextFlags.minStartingCards > nextFlags.maxStartingCards
            || typeof nextFlags.autoFillQueueWithBots !== 'boolean') {
            return res.status(400).json({ error: 'Invalid feature flag values.' });
        }
        const wasMaintenanceMode = featureFlags.maintenanceMode;
        featureFlags = nextFlags;
        if (db) await db.collection('adminConfig').doc('flags').set(featureFlags, { merge: true });
        await auditAdminAction(req.admin, 'update_flags', 'global', featureFlags);
        io.emit('feature_flags_updated', featureFlags);
        io.emit('maintenance_status', { maintenanceMode: featureFlags.maintenanceMode });
        if (featureFlags.maintenanceMode && !wasMaintenanceMode) {
            broadcastAnnouncement(createAnnouncement({
                message: 'Scheduled maintenance in progress. Online matches are temporarily unavailable.',
                priority: 'urgent',
                sticky: true,
                id: 'maintenance',
            }));
        } else if (!featureFlags.maintenanceMode && wasMaintenanceMode) {
            removeAnnouncement('maintenance');
        }
        res.json({ flags: featureFlags });
    } catch (error) {
        next(error);
    }
});

app.post('/api/admin/matchmaking', adminAuth, async (req, res, next) => {
    try {
        if (typeof req.body?.paused !== 'boolean') return res.status(400).json({ error: 'paused must be a boolean.' });
        matchmakingPaused = req.body.paused;
        await auditAdminAction(req.admin, matchmakingPaused ? 'pause_matchmaking' : 'resume_matchmaking', 'queue');
        res.json({ matchmakingPaused });
    } catch (error) {
        next(error);
    }
});

app.delete('/api/admin/queue', adminAuth, async (req, res, next) => {
    try {
        const count = queue.length;
        for (const entry of queue.splice(0)) {
            emitActionError(entry.socket, 'Matchmaking queue was cleared by an administrator.');
        }
        await auditAdminAction(req.admin, 'clear_queue', 'queue', { count });
        res.json({ cleared: count });
    } catch (error) {
        next(error);
    }
});

app.delete('/api/admin/queue/:uid', adminAuth, async (req, res, next) => {
    try {
        const uid = String(req.params.uid || '');
        const index = queue.findIndex((entry) => entry.playerData.uid === uid);
        if (index < 0) return res.status(404).json({ error: 'Player is not in the matchmaking queue.' });
        const [entry] = queue.splice(index, 1);
        emitActionError(entry.socket, 'You were removed from matchmaking by an administrator.');
        await auditAdminAction(req.admin, 'remove_from_queue', uid);
        res.json({ success: true });
    } catch (error) {
        next(error);
    }
});

async function handleAdminBroadcast(req, res, next) {
    try {
        const announcement = createAnnouncement({ ...(req.body || {}), id: undefined });
        broadcastAnnouncement(announcement);
        await auditAdminAction(req.admin, 'broadcast_announcement', 'all_players', {
            id: announcement.id,
            priority: announcement.priority,
            sticky: announcement.sticky,
            expiresAt: announcement.expiresAt,
        });
        res.json({ announcement });
    } catch (error) {
        if (error instanceof Error && /Announcement/.test(error.message)) {
            return res.status(400).json({ error: error.message });
        }
        next(error);
    }
}

app.post('/api/admin/broadcast', adminAuth, handleAdminBroadcast);
app.post('/api/admin/announcement', adminAuth, handleAdminBroadcast);

app.get('/api/admin/announcements', adminAuth, (req, res) => {
    res.json({ announcements: pruneNotices() });
});

app.delete('/api/admin/announcements/:id', adminAuth, async (req, res, next) => {
    try {
        const id = String(req.params.id || '');
        if (!removeAnnouncement(id)) return res.status(404).json({ error: 'Announcement not found.' });
        await auditAdminAction(req.admin, 'remove_announcement', id);
        res.json({ success: true, announcements: [...latestNotices] });
    } catch (error) {
        next(error);
    }
});

app.delete('/api/admin/announcements', adminAuth, async (req, res, next) => {
    try {
        const count = latestNotices.length;
        latestNotices.splice(0);
        latestAnnouncement = null;
        io.emit('announcement_cleared', { all: true });
        io.emit('server_announcements', []);
        await auditAdminAction(req.admin, 'clear_announcements', 'all_players', { count });
        res.json({ success: true, removed: count });
    } catch (error) {
        next(error);
    }
});

app.post('/api/admin/rooms/:roomId/action', adminAuth, async (req, res, next) => {
    try {
        const roomId = String(req.params.roomId || '');
        const game = activeGames[roomId];
        if (!game) return res.status(404).json({ error: 'Room not found.' });
        const action = req.body?.action;
        if (action === 'close') {
            if (game.state === 'playing') {
                await persistAdminEndedGame(game, { resultType: 'admin_closed' });
            }
            emitRoomClosed(game, 'This room was closed by an administrator.');
        } else if (action === 'promote_host') {
            const player = game.players.find((entry) => entry.uid === req.body?.uid);
            if (!player) return res.status(400).json({ error: 'Target player is not in this room.' });
            game.hostUid = player.uid;
            syncHostState(game);
            broadcastRoom(game);
            io.to(roomId).emit('host_changed', { roomId, newHostUid: player.uid, newHostName: player.name, hostUid: player.uid });
        } else if (action === 'pause' || action === 'resume') {
            if (game.state !== 'playing') return res.status(409).json({ error: 'Only active matches can be paused or resumed.' });
            game.adminPaused = action === 'pause';
            broadcastGameState(game);
        } else if (action === 'skip_turn') {
            if (game.state !== 'playing') return res.status(409).json({ error: 'Only active matches have a turn to skip.' });
            game.turnIndex = (game.turnIndex + 1) % game.players.length;
            game.pendingPenalty = 0;
            game.penaltyType = null;
            game.hasDrawnThisTurn = false;
            broadcastGameState(game);
        } else if (action === 'force_draw') {
            if (game.state !== 'playing') return res.status(409).json({ error: 'Only active matches can draw cards.' });
            const player = game.players.find((entry) => entry.uid === req.body?.uid);
            if (!player) return res.status(400).json({ error: 'Target player is not in this room.' });
            drawFromMarket(game, player.uid, Math.max(1, Math.min(5, Number(req.body?.count) || 1)));
            broadcastGameState(game);
        } else if (action === 'reshuffle_market') {
            if (game.state !== 'playing' || !Array.isArray(game.market)) return res.status(409).json({ error: 'The room has no active market to reshuffle.' });
            game.market = shuffleCards(game.market);
            broadcastGameState(game);
        } else if (action === 'kick') {
            const player = game.players.find((entry) => entry.uid === req.body?.uid);
            if (!player) return res.status(400).json({ error: 'Target player is not in this room.' });
            const targetSocket = io.sockets.sockets.get(player.socketId);
            if (targetSocket) {
                targetSocket.emit('room_closed', { roomId, message: 'You were removed from this room by an administrator.' });
                targetSocket.leave(roomId);
                targetSocket.data.roomId = null;
            }
            game.players = game.players.filter((entry) => entry.uid !== player.uid);
            if (!game.players.length) {
                if (game.state === 'playing') {
                    await persistAdminEndedGame(game, { resultType: 'admin_kick' });
                    io.to(roomId).emit('game_ended', { roomId, winnerUid: null, resultType: 'admin_kick' });
                }
                delete activeGames[roomId];
            }
            else {
                if (game.hostUid === player.uid) game.hostUid = game.players[0].uid;
                if (game.state === 'playing') {
                    game.state = 'ended';
                    game.status = 'ended';
                    game.winnerUid = game.players.length === 1 ? game.players[0].uid : null;
                    await persistAdminEndedGame(game, { winnerUid: game.winnerUid, resultType: 'admin_kick' });
                    io.to(roomId).emit('game_ended', { roomId, winnerUid: game.winnerUid, resultType: 'admin_kick' });
                    delete activeGames[roomId];
                } else broadcastRoom(game);
            }
        } else if (action === 'force_end') {
            if (game.state !== 'playing') return res.status(409).json({ error: 'Only active games can be force-ended.' });
            const winnerUid = req.body?.winnerUid || null;
            if (winnerUid && !game.players.some((entry) => entry.uid === winnerUid)) {
                return res.status(400).json({ error: 'Winner must be a player in this room.' });
            }
            await persistAdminEndedGame(game, { winnerUid, resultType: 'admin_forced' });
            io.to(roomId).emit('game_ended', { roomId, winnerUid, resultType: 'admin_forced' });
            delete activeGames[roomId];
        } else {
            return res.status(400).json({ error: 'Unknown room action.' });
        }
        await auditAdminAction(req.admin, action, roomId, { uid: req.body?.uid || null, winnerUid: req.body?.winnerUid || null });
        res.json({ success: true, rooms: Object.values(activeGames).map(publicRoomSummary) });
    } catch (error) {
        next(error);
    }
});

app.get('/api/rooms', adminAuth, (req, res) => {
    res.json({ rooms: Object.values(activeGames).map(publicRoomSummary), ...getServerStats() });
});

const featureFlagsReady = loadFeatureFlags();

// 404 Handler
app.use((req, res) => {
    res.status(404).json({ error: 'Route not found', path: req.originalUrl });
});

// Express Error Handler
app.use((err, req, res, next) => {
    log('💥', `Express error: ${err.message}`);
    res.status(500).json({ error: 'Internal server error' });
});

// ─── Socket.io Gameplay ───
io.on('connection', (socket) => {
    log('🟢', `Player connected: ${socket.id}`);
    socket.emit('server_announcements', pruneNotices());
    if (announcementIsActive(latestAnnouncement)) socket.emit('server_announcement', latestAnnouncement);
    socket.emit('feature_flags_updated', featureFlags);
    socket.emit('maintenance_status', { maintenanceMode: Boolean(featureFlags.maintenanceMode) });
    socket.data.requestStartedAt = Date.now();
    socket.data.requestCount = 0;
    socket.data.throttledUntil = 0;
    trackConnection(socket);
    socket.use((packet, next) => {
        const now = Date.now();
        if (now < socket.data.throttledUntil) return;
        if (now - socket.data.requestStartedAt >= 1000) {
            socket.data.requestStartedAt = now;
            socket.data.requestCount = 0;
        }
        socket.data.requestCount += 1;
        if (socket.data.requestCount > 10) {
            socket.data.throttledUntil = now + 5000;
            recordSecurityEvent('RATE_LIMIT_EXCEEDED', 'WARNING', {
                uid: socket.playerData?.uid,
                name: socket.playerData?.name,
                ip: clientIp(socket),
                roomId: socket.data.roomId,
                details: 'More than 10 client events in one second; socket throttled for five seconds.',
            });
            socket.emit('action_error', { error: 'Too many actions. Please wait five seconds.' });
            return;
        }
        next();
    });

    async function resolveRoomPlayer(playerData) {
        if (!playerData || typeof playerData !== 'object') throw new Error('Invalid player data.');
        const decoded = firebaseReady ? await verifyPlayerToken(playerData.idToken) : null;
        const uid = firebaseReady
            ? decoded.uid
            : (isNonEmptyString(playerData.uid, 200) ? playerData.uid.trim() : '');
        if (!uid) throw new Error('Unable to verify player identity.');
        const email = decoded?.email || '';
        const name = playerData.name || decoded?.name || (email ? email.split('@')[0] : 'Player');
        const photoURL = isNonEmptyString(playerData.photoURL, 2048) ? playerData.photoURL : (decoded?.picture || null);
        const isAdmin = Boolean(ADMIN_EMAIL && email.toLowerCase() === ADMIN_EMAIL);
        await assertPlayerAllowed(uid);
        const cosmetics = await loadPlayerCosmetics(uid, photoURL);
        const profilePhoto = cosmetics.photoURL;
        delete cosmetics.photoURL;
        const ip = clientIp(socket);
        socket.playerData = { uid, name, email, photoURL: profilePhoto, ...cosmetics, isAdmin, ip };
        trackConnection(socket, { uid, name });
        return { uid, name, email, photoURL: profilePhoto, ...cosmetics, isAdmin, isHost: false, socketId: socket.id, ip };
    }

    socket.on('register_player_presence', async (data, callback) => {
        try {
            const decoded = firebaseReady ? await verifyPlayerToken(data?.idToken) : null;
            if (!decoded?.uid) throw new Error('Player authentication is required.');
            const email = decoded.email || '';
            const name = decoded.name || (email ? email.split('@')[0] : 'Player');
            const photoURL = decoded.picture || null;
            const ip = clientIp(socket);
            const isAdmin = Boolean(ADMIN_EMAIL && email.toLowerCase() === ADMIN_EMAIL);
            socket.playerData = { uid: decoded.uid, name, email, photoURL, isAdmin, ip };
            trackConnection(socket, { uid: decoded.uid, name });
            if (typeof callback === 'function') callback({ success: true });
        } catch (error) {
            if (typeof callback === 'function') callback({ error: error.message || 'Player authentication failed.' });
        }
    });

    socket.on('create_room', async (playerData, callback) => {
        try {
            const player = await resolveRoomPlayer(playerData);
            if (featureFlags.maintenanceMode && !player.isAdmin) throw new Error('Online play is temporarily unavailable for maintenance.');
            const roomId = String(playerData.roomId || '').trim().toUpperCase();
            if (!isNonEmptyString(roomId, 24)) throw new Error('A valid room code is required.');
            if (activeGames[roomId]) throw new Error('That room code is already in use.');
            player.isHost = true;
            const room = activeGames[roomId] = { roomId, hostUid: player.uid, players: [player], state: 'waiting',
                settings: { ...DEFAULT_SETTINGS }, turnIndex: 0, createdAt: new Date().toISOString() };
            room.players.forEach((entry) => { entry.isHost = entry.uid === room.hostUid; });
            socket.join(roomId);
            socket.data.roomId = roomId;
            broadcastRoom(room);
            if (typeof callback === 'function') callback({ success: true, roomId });
        } catch (error) {
            emitActionError(socket, error.message);
            if (typeof callback === 'function') callback({ error: error.message });
        }
    });

    socket.on('join_room', async (playerData, callback) => {
        try {
            const player = await resolveRoomPlayer(playerData);
            if (featureFlags.maintenanceMode && !player.isAdmin) throw new Error('Online play is temporarily unavailable for maintenance.');
            const roomId = String(playerData.roomId || '').trim().toUpperCase();
            const room = activeGames[roomId];
            if (!room || room.state !== 'waiting') throw new Error('Room not found or game already started.');
            if (room.players.length >= Math.min(room.settings.maxPlayers, featureFlags.maxPlayers)) {
                throw new Error(`Room is full (${Math.min(room.settings.maxPlayers, featureFlags.maxPlayers)} players maximum).`);
            }
            if (room.players.some((existing) => existing.uid === player.uid)) throw new Error('You are already in this room.');
            const colliding = room.players.find((existing) => existing.ip && existing.ip === player.ip);
            if (colliding) {
                recordSecurityEvent('SAME_IP_COLLUSION', 'WARNING', {
                    uid: player.uid,
                    name: player.name,
                    ip: player.ip,
                    roomId,
                    details: `Same client IP as ${colliding.name} (${colliding.uid}) joined this room.`,
                });
            }
            room.players.push(player);
            syncHostState(room);
            socket.join(roomId);
            socket.data.roomId = roomId;
            broadcastRoom(room);
            if (typeof callback === 'function') callback({ success: true, roomId });
        } catch (error) {
            emitActionError(socket, error.message);
            if (typeof callback === 'function') callback({ error: error.message });
        }
    });

    socket.on('update_room_settings', (data) => {
        const room = data && activeGames[data.roomId];
        if (!room || room.state !== 'waiting' || !room.players.some((player) => player.socketId === socket.id && player.uid === room.hostUid)) {
            emitActionError(socket, 'Only the host can change settings before the game starts.');
            return;
        }
        const handSize = Number(data.startingHandSize);
        const decks = Number(data.decks);
        if (handSize < featureFlags.minStartingCards || handSize > featureFlags.maxStartingCards || ![1, 2].includes(decks)) {
            emitActionError(socket, 'Choose 4–8 starting cards and 1 or 2 decks.');
            return;
        }
        room.settings = { ...room.settings, startingHandSize: handSize, decks };
        broadcastRoom(room);
    });

    socket.on('start_game', async (data) => {
        if (featureFlags.maintenanceMode && !socket.playerData?.isAdmin) {
            emitActionError(socket, 'Online play is temporarily unavailable for maintenance.');
            return;
        }
        const room = data && activeGames[data.roomId];
        if (!room || room.state !== 'waiting' || !room.players.some((player) => player.socketId === socket.id && player.uid === room.hostUid)) {
            emitActionError(socket, 'Only the host can start this room.');
            return;
        }
        const playerCount = room.players.length;
        if (!featureFlags.rankedEnabled) room.settings.ranked = false;
        room.settings.maxPlayers = Math.min(room.settings.maxPlayers, featureFlags.maxPlayers);
        if (playerCount > featureFlags.maxPlayers) {
            emitActionError(socket, `This server currently allows up to ${featureFlags.maxPlayers} players per room.`);
            return;
        }
        const neededCards = playerCount * room.settings.startingHandSize + 1;
        const deckSize = 54 * room.settings.decks;
        if (playerCount < 2 || playerCount > 6 || neededCards >= deckSize) {
            emitActionError(socket, 'Not enough cards for this setup. Choose fewer cards or 2 decks.');
            return;
        }
        room.state = 'playing';
        room.ranked = featureFlags.rankedEnabled;
        room.turnSerial = 0;
        const startingPlayerIndex = Math.floor(Math.random() * playerCount);
        room.turnIndex = startingPlayerIndex;
        const deal = createDeal(room);
        if (deal.discardTop.value === 2) { room.pendingPenalty = 2; room.penaltyType = 'two'; }
        else if (deal.discardTop.value === 5) { room.pendingPenalty = 3; room.penaltyType = 'three'; }
        else if (deal.discardTop.value === 8) room.turnIndex = (room.turnIndex + 2) % playerCount;
        else if (deal.discardTop.value === 14) room.players.forEach((player) => { if (player.uid !== room.players[room.turnIndex].uid) drawFromMarket(room, player.uid, 1); });
        metrics.gamesStarted += 1;
        metrics.lastGameAt = new Date().toISOString();
        try {
            await createReplayMatch(room);
        } catch (error) {
            log('⚠️', `Could not initialize match replay: ${error.message}`);
        }
        const payload = { roomId: room.roomId, matchId: room.matchId || null, players: publicRoom(room).players, settings: room.settings,
            startingPlayerIndex: room.turnIndex, startingPlayerUid: room.players[room.turnIndex].uid, startsFirst: room.players[room.turnIndex].uid === socket.playerData?.uid,
            discardTop: deal.discardTop, marketCount: deal.marketCount };
        io.to(room.roomId).emit('game_started', payload);
        room.players.forEach((player) => io.to(player.socketId).emit('initial_hand', { roomId: room.roomId, hand: deal.handsByUid[player.uid] }));
        broadcastGameState(room);
        broadcastRoom(room);
    });

    socket.on('leave_room', (data) => {
        const room = data && activeGames[data.roomId];
        if (!room) return;
        const leaving = room.players.find((player) => player.socketId === socket.id || player.uid === socket.playerData?.uid);
        if (!leaving) return;
        const hostChanged = leaving.uid === room.hostUid;
        room.players = room.players.filter((player) => player.socketId !== socket.id && player.uid !== leaving.uid);
        socket.leave(room.roomId);
        socket.data.roomId = null;
        if (!room.players.length) {
            delete activeGames[room.roomId];
            return;
        }
        if (hostChanged) {
            room.hostUid = room.players[0].uid;
            room.players.forEach((player) => { player.isHost = player.uid === room.hostUid; });
            io.to(room.roomId).emit('host_changed', { roomId: room.roomId, newHostUid: room.hostUid, newHostName: room.players[0].name, hostUid: room.hostUid });
        }
        syncHostState(room);
        broadcastRoom(room);
    });

    // Join matchmaking
    socket.on('join_queue', async (playerData, callback) => {
        try {
            if (matchmakingPaused) throw new Error('Matchmaking is temporarily paused.');
            if (!playerData || typeof playerData !== 'object') {
                const errMsg = 'Invalid player data.';
                if (typeof callback === 'function') callback({ error: errMsg });
                return;
            }

            if (queue.some((q) => q.socket.id === socket.id)) {
                if (typeof callback === 'function') callback({ error: 'You are already in the queue.' });
                return;
            }

            const existingRoom = Object.values(activeGames).find((g) =>
                g.players.some((p) => p.socketId === socket.id)
            );
            if (existingRoom) {
                if (typeof callback === 'function') callback({ error: 'You are already in an active game.' });
                return;
            }

            let uid = '';
            let name = '';
            let email = '';
            let photoURL = null;

            if (firebaseReady) {
                const decoded = await verifyPlayerToken(playerData.idToken);
                uid = decoded.uid;
                email = decoded.email || '';
                name = playerData.name || decoded.name || (email ? email.split('@')[0] : 'Player');
                photoURL = isNonEmptyString(playerData.photoURL, 2048)
                    ? playerData.photoURL
                    : (decoded.picture || null);
            } else {
                if (!isNonEmptyString(playerData.name) || !isNonEmptyString(playerData.uid)) {
                    const errMsg = 'Name and uid are required when Firebase token is not provided.';
                    if (typeof callback === 'function') callback({ error: errMsg });
                    return;
                }
                uid = playerData.uid.trim();
                name = playerData.name.trim();
                photoURL = isNonEmptyString(playerData.photoURL, 2048) ? playerData.photoURL : null;
            }

            const isAdmin = Boolean(ADMIN_EMAIL && email.toLowerCase() === ADMIN_EMAIL);
            await assertPlayerAllowed(uid);
            const cosmetics = await loadPlayerCosmetics(uid, photoURL);
            const ip = clientIp(socket);
            socket.playerData = { uid, name, email, ...cosmetics, isAdmin, ip };
            trackConnection(socket, { uid, name });
            if (featureFlags.maintenanceMode && !isAdmin) throw new Error('Online play is temporarily unavailable for maintenance.');

            if (queue.length > 0) {
                const opponent = queue.shift();
                const roomId = `room_${Date.now().toString().slice(-4)}_${Math.random()
                    .toString(36)
                    .slice(2, 5)}`;

                socket.join(roomId);
                opponent.socket.join(roomId);

                const roomPlayers = [
                    { socketId: opponent.socket.id, ...opponent.playerData, isHost: true },
                    { socketId: socket.id, ...socket.playerData, isHost: false },
                ];
                if (roomPlayers[0].ip && roomPlayers[0].ip === roomPlayers[1].ip) {
                    recordSecurityEvent('SAME_IP_COLLUSION', 'WARNING', {
                        uid,
                        name,
                        ip,
                        roomId,
                        details: `Queued opponents ${roomPlayers[0].name} and ${roomPlayers[1].name} share a client IP.`,
                    });
                }

                if (roomPlayers.length > MAX_PLAYERS_PER_ROOM) {
                    if (typeof callback === 'function') callback({ error: 'Room is full.' });
                    return;
                }

                activeGames[roomId] = {
                    roomId,
                    hostUid: roomPlayers[0].uid,
                    players: roomPlayers,
                    state: 'waiting',
                    settings: { ...DEFAULT_SETTINGS, ...suggestSettings(roomPlayers.length) },
                    turnIndex: 0,
                    createdAt: new Date().toISOString(),
                };
                socket.data.roomId = roomId;
                opponent.socket.data.roomId = roomId;
                broadcastRoom(activeGames[roomId]);

                log('🎮', `Waiting room created: ${roomId} (${roomPlayers.map((p) => p.name).join(' vs ')})`);
                if (typeof callback === 'function') callback({ success: true, roomId });
            } else {
                queue.push({ socket, playerData: socket.playerData, joinedAt: new Date().toISOString() });
                socket.emit('waiting', { message: 'Searching for online players...' });
                log('⏳', `${socket.playerData.name} entered queue. Queue size: ${queue.length}`);
                if (typeof callback === 'function') callback({ success: true, queued: true });
            }
        } catch (err) {
            log('💥', `join_queue error: ${err.message}`);
            if (typeof callback === 'function') {
                callback({ error: err.message || 'Server error while joining queue.' });
            }
            emitActionError(socket, err.message || 'Auth/join failed.');
        }
    });

    socket.on('leave_queue', () => {
        const idx = queue.findIndex((q) => q.socket.id === socket.id);
        if (idx !== -1) {
            queue.splice(idx, 1);
            socket.emit('left_queue', { message: 'You left the matchmaking queue.' });
            log('🚪', `${socket.playerData?.name || socket.id} left the queue.`);
        }
    });

    function validateRoomAction(data) {
        if (!data || !isNonEmptyString(data.roomId)) return false;
        const game = activeGames[data.roomId];
        if (!game) return false;
        return game.players.some((p) => p.socketId === socket.id);
    }

    function activeGameForTurn(data) {
        if (!validateRoomAction(data)) return null;
        const game = activeGames[data.roomId];
        const actor = game.players.find((player) => player.socketId === socket.id);
        if (!actor || game.state !== 'playing' || game.status !== 'playing' || game.adminPaused) return null;
        if (game.players[game.turnIndex]?.socketId !== socket.id) {
            recordSecurityEvent('OUT_OF_TURN_ACTION', 'WARNING', {
                uid: actor.uid,
                name: actor.name,
                ip: actor.ip,
                roomId: game.roomId,
                details: `Action arrived while seat ${game.turnIndex} owned the turn.`,
            });
            return null;
        }
        return { game, actor };
    }

    socket.on('play_card', async (data) => {
        try {
            const turn = activeGameForTurn(data);
            if (!turn) { emitActionError(socket, 'It is not your turn or this game is no longer active.', 'game_action_error'); return; }
            const { game, actor } = turn;
            const hand = game.handsByUid[actor.uid] || [];
            const turnIndexBefore = game.turnIndex;
            const analysisContext = captureReplayContext(game, actor.uid);
            const cardIndex = hand.findIndex((card) => card.id === data.cardId);
            if (cardIndex < 0) {
                recordSecurityEvent('DESYNC_HAND_TAMPERING', 'CRITICAL', {
                    uid: actor.uid,
                    name: actor.name,
                    ip: actor.ip,
                    roomId: game.roomId,
                    details: `Submitted card ${String(data.cardId || '').slice(0, 80)} is absent from the server-recorded hand.`,
                });
                emitActionError(socket, 'That card is not in your hand.', 'game_action_error');
                return;
            }
            const card = hand[cardIndex];
            const topCard = game.discardPile[game.discardPile.length - 1];
            const requestedShape = String(data.requestedShape ?? data.namedSuit ?? '').toLowerCase();
            const validShapes = ['circles', 'triangles', 'crosses', 'squares', 'stars'];
            const wild = isWildCard(card);
            if (wild && !validShapes.includes(requestedShape)) { emitActionError(socket, 'Choose a shape when playing Whot.', 'game_action_error'); return; }
            if (wild && (game.announcedShape?.uid !== actor.uid || String(game.announcedShape?.shape ?? '').toLowerCase() !== requestedShape)) { emitActionError(socket, 'Call a shape before playing Whot.', 'game_action_error'); return; }
            const cardValue = cardNumber(card);
            const topValue = cardNumber(topCard);
            const cardSuit = cardShape(card);
            const topSuit = cardShape(topCard);
            const activeShape = game.activeSuit ? String(game.activeSuit).toLowerCase() : null;
            const legal = game.pendingPenalty > 0
                ? wild || (game.penaltyType === 'two' ? cardValue === 2 : cardValue === 5)
                : wild || (activeShape ? cardSuit === activeShape || cardValue === topValue : cardSuit === topSuit || cardValue === topValue);
            if (!legal) { emitActionError(socket, 'That card cannot be played on the current discard.', 'game_action_error'); return; }

            metrics.cardsPlayed += 1;
            recordGameplayAction(socket, data, 'PLAY_CARD', {
                card: { label: card.label, value: card.value, suit: card.suit },
            });
            if (hand.length === 2 && !actor.calledLastCard) drawFromMarket(game, actor.uid, 1);
            hand.splice(cardIndex, 1);
            actor.calledLastCard = false;
            game.discardPile.push(card);
            game.playedCount += 1;
            if (wild) game.activeSuit = requestedShape;
            game.hasDrawnThisTurn = false;
            const totalPlayers = game.players.length;
            if (hand.length === 0) {
                game.status = 'ended';
                game.state = 'ended';
                game.winnerUid = actor.uid;
            } else {
                const next = (game.turnIndex + 1) % totalPlayers;
                if (cardValue === 2) { game.pendingPenalty += 2; game.penaltyType = 'two'; game.turnIndex = next; }
                else if (cardValue === 5) { game.pendingPenalty += 3; game.penaltyType = 'three'; game.turnIndex = next; }
                else if (cardValue === 1) { game.turnIndex = game.turnIndex; }
                else if (cardValue === 8) { game.turnIndex = (game.turnIndex + (cardSuit === 'stars' ? 3 : 2)) % totalPlayers; }
                else if (cardValue === 14) {
                    game.players.forEach((player) => { if (player.uid !== actor.uid) drawFromMarket(game, player.uid, 1); });
                    game.turnIndex = next;
                } else if (wild) {
                    game.turnIndex = next;
                } else {
                    game.activeSuit = null;
                    game.turnIndex = next;
                }
            }

            game.announcedShape = null;
            game.turnSerial = (game.turnSerial || 0) + 1;
            void recordReplayTurn(game, {
                actionType: wild ? 'whot' : 'play',
                actorUid: actor.uid,
                actorName: actor.name,
                card,
                requestedShape: wild ? requestedShape : null,
                turnIndexBefore,
                analysisContext,
            });
            if (hand.length === 0) {
                await finalizeReplayMatch(game, { winnerUid: actor.uid, resultType: 'normal' });
            }
            socket.to(data.roomId).emit('opponent_played_card', {
                roomId: data.roomId,
                playerId: actor.uid,
                senderUid: actor.uid,
                card,
                requestedShape: wild ? requestedShape : null,
                nextTurnIndex: game.turnIndex,
            });
            if (hand.length === 0) {
                io.to(data.roomId).emit('game_ended', { roomId: data.roomId, winnerUid: actor.uid });
            }
            broadcastGameState(game);
        } catch (err) { log('💥', `play_card error: ${err.message}`); }
    });

    socket.on('draw_card', async (data) => {
        try {
            const turn = activeGameForTurn(data);
            if (!turn) { emitActionError(socket, 'It is not your turn or this game is no longer active.', 'game_action_error'); return; }
            const { game, actor } = turn;
            if (game.hasDrawnThisTurn && game.pendingPenalty === 0) {
                emitActionError(socket, 'You already drew a card. Play a card or pass your turn.', 'game_action_error');
                return;
            }
            const count = game.pendingPenalty > 0 ? game.pendingPenalty : 1;
            const turnIndexBefore = game.turnIndex;
            const wasPenalty = game.pendingPenalty > 0;
            const analysisContext = captureReplayContext(game, actor.uid);
            recordGameplayAction(socket, data, wasPenalty ? 'PENALTY_DRAW' : 'DRAW_CARD', { count });
            drawFromMarket(game, actor.uid, count);
            actor.calledLastCard = false;
            if (game.pendingPenalty > 0) {
                game.pendingPenalty = 0;
                game.penaltyType = null;
                game.hasDrawnThisTurn = false;
                game.turnIndex = (game.turnIndex + 1) % game.players.length;
                game.turnSerial = (game.turnSerial || 0) + 1;
            } else {
                game.hasDrawnThisTurn = true;
            }
            void recordReplayTurn(game, {
                actionType: wasPenalty ? 'penalty_draw' : 'draw',
                actorUid: actor.uid,
                actorName: actor.name,
                penaltyAmount: wasPenalty ? count : null,
                turnIndexBefore,
                analysisContext,
            });
            socket.to(data.roomId).emit('opponent_drew_card', { roomId: data.roomId, playerId: actor.uid, count });
            broadcastGameState(game);
        } catch (err) { log('💥', `draw_card error: ${err.message}`); }
    });

    socket.on('pass_turn', async (data) => {
        const turn = activeGameForTurn(data);
        if (!turn) { emitActionError(socket, 'It is not your turn or this game is no longer active.', 'game_action_error'); return; }
        const { game } = turn;
        recordGameplayAction(socket, data, 'PASS_TURN');
        const turnIndexBefore = game.turnIndex;
        const analysisContext = captureReplayContext(game, turn.actor.uid);
        if (!game.hasDrawnThisTurn || game.pendingPenalty > 0) {
            emitActionError(socket, 'Draw a card before passing.', 'game_action_error');
            return;
        }
        game.hasDrawnThisTurn = false;
        game.turnIndex = (game.turnIndex + 1) % game.players.length;
        game.turnSerial = (game.turnSerial || 0) + 1;
        void recordReplayTurn(game, {
            actionType: 'pass',
            actorUid: turn.actor.uid,
            actorName: turn.actor.name,
            turnIndexBefore,
            analysisContext,
        });
        broadcastGameState(game);
    });

    socket.on('call_last_card', (data) => {
        const turn = activeGameForTurn(data);
        if (!turn || (turn.game.handsByUid[turn.actor.uid] || []).length !== 2) {
            emitActionError(socket, 'Call Last Card when you have exactly two cards.', 'game_action_error');
            return;
        }
        turn.actor.calledLastCard = !turn.actor.calledLastCard;
        recordGameplayAction(socket, data, 'CALL_LAST_CARD');
        broadcastGameState(turn.game);
    });

    socket.on('call_whot', async (data) => {
        const turn = activeGameForTurn(data);
        const shape = data.namedSuit || data.newShape;
        const whotInHand = turn && (turn.game.handsByUid[turn.actor.uid] || []).some((card) => card.id === data.cardId && card.value === 20);
        if (!turn || !whotInHand || !['circles', 'triangles', 'crosses', 'squares', 'stars'].includes(shape)) {
            emitActionError(socket, 'Invalid Whot shape call.', 'game_action_error');
            return;
        }
        turn.game.announcedShape = { uid: turn.actor.uid, shape };
        recordGameplayAction(socket, data, 'CALL_WHOT', { requestedShape: String(shape).toLowerCase() });
        void recordReplayTurn(turn.game, {
            actionType: 'whot',
            actorUid: turn.actor.uid,
            actorName: turn.actor.name,
            requestedShape: shape,
            turnIndexBefore: turn.game.turnIndex,
            analysisContext: captureReplayContext(turn.game, turn.actor.uid),
        });
        socket.to(data.roomId).emit('opponent_called_whot', { roomId: data.roomId, playerId: turn.actor.uid, namedSuit: shape });
    });

    socket.on('voice_offer', (data) => {
        if (!featureFlags.voiceEnabled || !validateRoomAction(data) || !data.sdp || !isNonEmptyString(data.sdp.sdp, 10000)) {
            emitActionError(socket, 'Invalid voice offer or room membership.');
            return;
        }
        metrics.voiceEventsRelayed += 1;
        socket.to(data.roomId).emit('voice_offer', { roomId: data.roomId, sdp: data.sdp });
    });

    socket.on('voice_ready', (data) => {
        if (!featureFlags.voiceEnabled || !validateRoomAction(data)) {
            emitActionError(socket, 'Invalid voice readiness signal or room membership.');
            return;
        }
        socket.to(data.roomId).emit('voice_ready', { roomId: data.roomId });
    });

    socket.on('voice_answer', (data) => {
        if (!featureFlags.voiceEnabled || !validateRoomAction(data) || !data.sdp || !isNonEmptyString(data.sdp.sdp, 10000)) {
            emitActionError(socket, 'Invalid voice answer or room membership.');
            return;
        }
        metrics.voiceEventsRelayed += 1;
        socket.to(data.roomId).emit('voice_answer', { roomId: data.roomId, sdp: data.sdp });
    });

    socket.on('ice_candidate', (data) => {
        if (!featureFlags.voiceEnabled || !validateRoomAction(data) || !data.candidate || typeof data.candidate.candidate !== 'string' || data.candidate.candidate.length > 4096) {
            emitActionError(socket, 'Invalid ICE candidate or room membership.');
            return;
        }
        metrics.voiceEventsRelayed += 1;
        socket.to(data.roomId).emit('ice_candidate', { roomId: data.roomId, candidate: data.candidate });
    });

    socket.on('game_over', async (data) => {
        try {
            if (!validateRoomAction(data)) return;

            const game = activeGames[data.roomId];
            if (game.ending) return;
            const winnerUid = game.winnerUid;
            if (game.state !== 'ended' || game.status !== 'ended'
                || !isNonEmptyString(winnerUid, 200)
                || !game.players.some((player) => player.uid === winnerUid)) {
                emitActionError(socket, 'The server has not confirmed this game result.');
                return;
            }
            game.ending = true;

            io.to(data.roomId).emit('game_ended', {
                roomId: data.roomId,
                winnerUid,
                matchId: game.matchId || null,
            });

            await finalizeReplayMatch(game, { winnerUid, resultType: 'normal' });
            metrics.gamesCompleted += 1;
            if (db && FieldValue && game && game.ranked !== false) {
                try {
                    const happyHourMultiplier = happyHourIsActive() ? happyHour.multiplier : 1;
                    const playersByUid = new Map(
                        game.players
                            .filter((player) => !player.isBot && isNonEmptyString(player.uid, 200))
                            .map((player) => [player.uid, player])
                    );
                    const players = [...playersByUid.values()];
                    const userRefs = players.map((player) => db.collection('users').doc(player.uid));
                    const matchRef = game.matchId
                        ? db.collection('matches').doc(game.matchId)
                        : db.collection('matches').doc();
                    const updatedAt = new Date().toISOString();

                    await db.runTransaction(async (transaction) => {
                        const userSnapshots = await Promise.all(userRefs.map((userRef) => transaction.get(userRef)));
                        transaction.set(matchRef, {
                            roomId: data.roomId,
                            players: game.matchPlayers || game.players.map((player) => ({
                                    uid: player.uid,
                                    name: player.name,
                                    ...(player.photoURL ? { photoURL: player.photoURL } : {}),
                                })),
                            participantUids: (game.matchPlayers || game.players).map((player) => player.uid),
                            winnerUid,
                            rankingPointsMultiplier: happyHourMultiplier,
                                ...(!game.matchId ? { createdAt: FieldValue.serverTimestamp() } : {}),
                            playedAt: FieldValue.serverTimestamp(),
                            ranked: true,
                        }, { merge: true });

                        userRefs.forEach((userRef, index) => {
                            const player = players[index];
                            const userData = userSnapshots[index].exists ? userSnapshots[index].data() : {};
                            const isWinner = player.uid === winnerUid;
                            const winStreak = typeof userData.winStreak === 'number' ? userData.winStreak : 0;
                            const bestWinStreak = typeof userData.bestWinStreak === 'number' ? userData.bestWinStreak : 0;
                            const progression = progressionForMatch(userData, isWinner);
                            transaction.set(userRef, {
                                uid: player.uid,
                                displayName: player.name,
                                ...(player.email ? { email: player.email } : {}),
                                ...(player.photoURL ? { photoURL: player.photoURL } : {}),
                                wins: FieldValue.increment(isWinner ? 1 : 0),
                                losses: FieldValue.increment(isWinner ? 0 : 1),
                                matchesPlayed: FieldValue.increment(1),
                                winStreak: isWinner ? FieldValue.increment(1) : 0,
                                bestWinStreak: isWinner ? Math.max(bestWinStreak, winStreak + 1) : bestWinStreak,
                                rankingPoints: FieldValue.increment(isWinner ? happyHourMultiplier : 0),
                                happyHourWins: FieldValue.increment(isWinner && happyHourMultiplier > 1 ? 1 : 0),
                                xp: FieldValue.increment(progression.xpEarned),
                                achievementsUnlocked: progression.achievementsUnlocked,
                                updatedAt,
                                lastPlayedAt: updatedAt,
                            }, { merge: true });
                        });
                    });
                    log('💾', `Match and leaderboard stats saved: ${data.roomId}`);
                } catch (saveErr) {
                    log('⚠️', `Failed to save match and stats: ${saveErr.message}`);
                }
            }

            delete activeGames[data.roomId];
            log('🏁', `Game ended in ${data.roomId}. Room closed.`);
        } catch (err) {
            log('💥', `game_over error: ${err.message}`);
        }
    });

    socket.on('get_admin_data', async (data, callback) => {
        try {
            await authenticateAdminSocket(socket, data?.idToken);
            const payload = await adminMetrics();
            payload.activeRooms = payload.roomSummaries;
            socket.emit('admin_data_response', payload);
            if (typeof callback === 'function') callback(payload);
        } catch (err) {
            if (typeof callback === 'function') callback({ error: err.message || 'Unauthorized.' });
        }
    });

    socket.on('get_security_logs', async (data, callback) => {
        try {
            await authenticateAdminSocket(socket, data?.idToken);
            const logs = [...securityLogs];
            socket.emit('security_logs_response', { logs });
            if (typeof callback === 'function') callback({ logs });
        } catch (error) {
            const response = { error: error.message || 'Unauthorized.' };
            socket.emit('security_logs_response', response);
            if (typeof callback === 'function') callback(response);
        }
    });

    socket.on('admin_authenticate', async (data, callback) => {
        try {
            const admin = await authenticateAdminSocket(socket, data?.idToken);
            await auditAdminAction(admin, 'admin_socket_authenticate', socket.id);
            if (typeof callback === 'function') callback({ success: true });
        } catch (error) {
            if (typeof callback === 'function') callback({ error: error.message || 'Unauthorized.' });
        }
    });

    socket.on('admin_ping', async (data, callback) => {
        try {
            await authenticateAdminSocket(socket, data?.idToken);
            if (typeof callback === 'function') callback({ serverTime: Date.now() });
        } catch (error) {
            if (typeof callback === 'function') callback({ error: error.message || 'Unauthorized.' });
        }
    });

    socket.on('admin_simulate_traffic', async (data, callback) => {
        try {
            const admin = await authenticateAdminSocket(socket, data?.idToken);
            const players = Number(data?.players);
            if (![10, 50].includes(players)) throw new Error('Simulation size must be 10 or 50 virtual players.');
            const totalSimulated = [...trafficSimulations.values()].reduce((sum, session) => sum + session.players, 0);
            if (totalSimulated + players > 100) throw new Error('Virtual traffic is capped at 100 concurrent players.');
            const id = `sim-${Date.now()}-${crypto.randomBytes(3).toString('hex')}`;
            const session = {
                players,
                rooms: Math.ceil(players / 2),
                expiresAt: Date.now() + 30_000,
                timer: null,
                virtualSockets: Array.from({ length: players }, (_, index) => ({
                    id: `${id}-socket-${index + 1}`,
                    connected: true,
                    joinedQueue: true,
                    eventsExchanged: 0,
                })),
            };
            session.timer = setInterval(() => {
                if (session.expiresAt <= Date.now()) return;
                const now = Date.now();
                session.virtualSockets.forEach((mockSocket) => {
                    if (!mockSocket.connected) return;
                    mockSocket.eventsExchanged += 1;
                    mockSocket.joinedQueue = !mockSocket.joinedQueue;
                });
                for (let index = 0; index < Math.min(10, players); index += 1) gameplayActionTimes.push(now);
            }, 1000);
            trafficSimulations.set(id, session);
            await auditAdminAction(admin, 'simulate_traffic', id, { players, durationSeconds: 30 });
            const response = { success: true, players, expiresAt: new Date(session.expiresAt).toISOString() };
            if (typeof callback === 'function') callback(response);
            sampleTelemetry();
        } catch (error) {
            if (typeof callback === 'function') callback({ error: error.message || 'Traffic simulation failed.' });
        }
    });

    socket.on('admin_send_direct_warning', async (data, callback) => {
        try {
            const admin = await authenticateAdminSocket(socket, data?.idToken);
            const targetUid = String(data?.targetUid || '');
            const message = String(data?.message || '').trim().slice(0, 300);
            if (!isNonEmptyString(targetUid, 200) || !message) throw new Error('A player ID and warning message are required.');
            let delivered = 0;
            for (const targetSocket of io.sockets.sockets.values()) {
                if (targetSocket.playerData?.uid === targetUid) {
                    targetSocket.emit('admin_direct_warning', { message, sentAt: new Date().toISOString() });
                    delivered += 1;
                }
            }
            if (!delivered) throw new Error('The target player is not currently connected.');
            await auditAdminAction(admin, 'send_direct_warning', targetUid, { message: message.slice(0, 120), delivered });
            if (typeof callback === 'function') callback({ success: true, delivered });
        } catch (error) {
            if (typeof callback === 'function') callback({ error: error.message || 'Direct warning failed.' });
        }
    });

    socket.on('admin_spectate_room', async (data, callback) => {
        try {
            const admin = await requireAdminToken(data?.idToken);
            const roomId = String(data?.roomId || '');
            const game = activeGames[roomId];
            if (!game) throw new Error('Room not found.');
            for (const [existingRoomId, spectators] of ghostSpectators) {
                spectators.delete(socket.id);
                if (!spectators.size) ghostSpectators.delete(existingRoomId);
            }
            if (!ghostSpectators.has(roomId)) ghostSpectators.set(roomId, new Set());
            ghostSpectators.get(roomId).add(socket.id);
            const snapshot = adminGameSnapshot(game);
            socket.emit('admin_ghost_snapshot', snapshot);
            await auditAdminAction(admin, 'spectate_room', roomId);
            if (typeof callback === 'function') callback({ success: true });
        } catch (error) {
            const response = { error: error.message || 'Unable to spectate room.' };
            socket.emit('admin_ghost_error', response);
            if (typeof callback === 'function') callback(response);
        }
    });

    socket.on('admin_stop_spectating', async (data, callback) => {
        try {
            const admin = await requireAdminToken(data?.idToken);
            for (const [roomId, spectators] of ghostSpectators) {
                spectators.delete(socket.id);
                if (!spectators.size) ghostSpectators.delete(roomId);
            }
            socket.emit('admin_ghost_snapshot', null);
            await auditAdminAction(admin, 'stop_spectating', String(data?.roomId || 'all'));
            if (typeof callback === 'function') callback({ success: true });
        } catch (error) {
            const response = { error: error.message || 'Unable to stop spectating.' };
            if (typeof callback === 'function') callback(response);
        }
    });

    const updateSocketBan = async (data, banned, callback) => {
        try {
            const admin = await requireAdminToken(data?.idToken);
            const uid = String(data?.targetUid || '');
            if (!db) throw new Error('Player moderation is unavailable.');
            if (!uid || uid.length > 200) throw new Error('A valid player ID is required.');
            await db.collection('users').doc(uid).set({
                banned,
                status: banned ? 'banned' : 'active',
                banUntil: null,
                banReason: banned ? String(data?.reason || 'Administrative moderation').slice(0, 300) : null,
                updatedAt: new Date().toISOString(),
            }, { merge: true });
            if (banned) await enforcePlayerBan(uid);
            const event = recordSecurityEvent(banned ? 'ADMIN_BAN' : 'ADMIN_UNBAN', 'INFO', {
                uid,
                details: `Account ${banned ? 'banned' : 'unbanned'} by ${admin.email}.`,
            });
            await auditAdminAction(admin, banned ? 'ban_player' : 'unban_player', uid);
            const response = { success: true, event };
            socket.emit('admin_ban_result', response);
            if (typeof callback === 'function') callback(response);
        } catch (error) {
            const response = { error: error.message || 'Moderation failed.' };
            socket.emit('admin_ban_result', response);
            if (typeof callback === 'function') callback(response);
        }
    };
    socket.on('admin_ban_user', (data, callback) => { void updateSocketBan(data, true, callback); });
    socket.on('admin_unban_user', (data, callback) => { void updateSocketBan(data, false, callback); });

    /** Relay a validated table emote to the other players in the sender's active room. */
    socket.on('player_emote', (data) => {
        const roomId = String(data?.roomId || '');
        const emote = String(data?.emote || '');
        const uid = socket.playerData?.uid;
        const game = activeGames[roomId];
        const allowedEmotes = new Set(['laugh', 'fire', 'skull', 'brain', 'flag', 'lightning']);
        if (!game || !uid || !allowedEmotes.has(emote) || !game.players.some((player) => player.uid === uid && player.socketId === socket.id)) return;
        const now = Date.now();
        if (socket.data.lastEmoteAt && now - socket.data.lastEmoteAt < 1200) return;
        socket.data.lastEmoteAt = now;
        socket.to(roomId).emit('opponent_emote', { roomId, uid, emote });
    });

    /** Remove queue, room, and ephemeral connection state after a socket disconnects. */
    socket.on('disconnect', async (reason) => {
        try {
            activeConnections.delete(socket.id);
            adminSocketSessions.delete(socket.id);
            for (const [roomId, spectators] of ghostSpectators) {
                spectators.delete(socket.id);
                if (!spectators.size) ghostSpectators.delete(roomId);
            }
            const qIdx = queue.findIndex((q) => q.socket.id === socket.id);
            if (qIdx !== -1) {
                queue.splice(qIdx, 1);
                log('🚪', `${socket.playerData?.name || socket.id} removed from queue (disconnect).`);
            }

            for (const [roomId, game] of Object.entries(activeGames)) {
                const player = game.players.find((p) => p.socketId === socket.id || p.uid === socket.playerData?.uid);
                if (!player) continue;

                game.players = game.players.filter((entry) => entry.socketId !== socket.id && entry.uid !== player.uid);

                if (!game.players.length) {
                    delete activeGames[roomId];
                    log('❌', `Room ${roomId} closed after last player disconnected (${reason}).`);
                    break;
                }

                if (game.state === 'playing' && game.players.length === 1) {
                    game.state = 'ended';
                    game.status = 'ended';
                    game.winnerUid = game.players[0].uid;
                    try {
                        await recordReplayTurn(game, {
                            actionType: 'disconnect',
                            actorUid: player.uid,
                            actorName: player.name,
                            turnIndexBefore: game.turnIndex,
                        });
                        await persistAdminEndedGame(game, { winnerUid: game.players[0].uid, resultType: 'disconnect_forfeit' });
                    } catch (error) {
                        log('⚠️', `Disconnect result persistence failed: ${error.message}`);
                    }
                    io.to(roomId).emit('game_ended', { roomId, winnerUid: game.players[0].uid, matchId: game.matchId || null });
                    broadcastRoom(game);
                    delete activeGames[roomId];
                    break;
                }

                const hostChanged = player.uid === game.hostUid;
                if (hostChanged) {
                    game.hostUid = game.players[0].uid;
                    game.players.forEach((entry) => { entry.isHost = entry.uid === game.hostUid; });
                    io.to(roomId).emit('host_changed', {
                        roomId,
                        newHostUid: game.hostUid,
                        newHostName: game.players[0].name,
                        hostUid: game.hostUid,
                    });
                }

                if (game.state === 'waiting') {
                    Object.assign(game.settings, suggestSettings(game.players.length));
                    broadcastRoom(game);
                } else if (game.state === 'playing') {
                    io.to(roomId).emit('opponent_disconnected', {
                        message: `${player.name} disconnected.`,
                        roomId,
                        uid: player.uid,
                    });
                    broadcastGameState(game);
                    broadcastRoom(game);
                }

                log('🔴', `Player disconnected from room ${roomId}: ${player.name} (${reason})`);
                break;
            }

            log('🔴', `Player disconnected: ${socket.id} (${reason})`);
        } catch (err) {
            log('💥', `disconnect cleanup error: ${err.message}`);
        }
    });

    socket.onAny((event) => {
        const knownEvents = [
            'join_queue',
            'create_room',
            'join_room',
            'leave_room',
            'update_room_settings',
            'start_game',
            'leave_queue',
            'play_card',
            'draw_card',
            'pass_turn',
            'call_last_card',
            'call_whot',
            'voice_offer',
            'voice_ready',
            'voice_answer',
            'ice_candidate',
            'game_over',
            'get_admin_data',
            'disconnect',
        ];
        if (!knownEvents.includes(event)) {
            log('⚠️', `Unknown event "${event}" from ${socket.id}`);
        }
    });
});

// ─── Graceful Shutdown ───
function shutdown(signal) {
    log('🛑', `${signal} received. Shutting down gracefully...`);
    io.emit('server_shutdown', {
        message: 'Server is restarting. Please reconnect in a moment.',
    });
    io.close();
    server.close(() => {
        log('✅', 'Server closed cleanly.');
        process.exit(0);
    });
    setTimeout(() => process.exit(1), 5000);
}

process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));

server.on('error', (err) => {
    if (err.code === 'EADDRINUSE') {
        log('💥', `Port ${PORT} is already in use. Close the other process or change PORT.`);
    } else {
        log('💥', `Server error: ${err.message}`);
    }
    process.exit(1);
});

// ─── Start Server & Activate Keep-Alive ───
featureFlagsReady.then(() => {
    server.listen(PORT, () => {
        log('🚀', `Whot Backend Server running on http://localhost:${PORT}`);
        log(
            'ℹ️',
            'REST: /  /health  /api/stats  /api/admin/* (Firebase administrator token required)'
        );

        startSelfPing();
    });
}).catch((error) => {
    log('💥', `Server startup failed: ${error.message}`);
    process.exit(1);
});


// --- FIRESTORE QUOTA 500 FALLBACK MIDDLEWARE ---
const RESOURCE_EXHAUSTED_FALLBACK = (err, req, res, next) => {
  if (err && (err.code === 8 || err.message?.includes('RESOURCE_EXHAUSTED') || err.message?.includes('Quota exceeded'))) {
    console.warn('⚠️ Intercepted Firestore quota error on ' + req.path + ' - returning degraded status 200 payload.');
    return res.status(200).json({
      degraded: true,
      error: 'Firestore quota exceeded. System operating in degraded mode.',
      players: [],
      auditLogs: [],
      securityLogs: [],
      metrics: { activeMatches: 0, connectedPlayers: 0 }
    });
  }
  next(err);
};
app.use('/api/admin', RESOURCE_EXHAUSTED_FALLBACK);


// --- EXPRESS FIRESTORE QUOTA 500 INTERCEPTOR ---
app.use((err, req, res, next) => {
  // EXPRESS_QUOTA_PROTECTION
  if (err && (err.code === 8 || err.message?.includes('RESOURCE_EXHAUSTED') || err.message?.includes('Quota exceeded'))) {
    console.warn('⚠️ Quota exceeded on ' + req.path + ' - returning degraded HTTP 200 payload.');
    return res.status(200).json({
      degraded: true,
      error: 'Firestore quota limit reached. Operating in degraded mode.',
      players: [],
      auditLogs: [],
      securityLogs: [],
      matches: [],
      metrics: { activeMatches: 0, connectedPlayers: 0 }
    });
  }
  next(err);
});


// EXPRESS_QUOTA_FALLBACK_MARK
app.use(function (err, req, res, next) {
  if (err && (err.code === 8 || String(err.message || '').includes('RESOURCE_EXHAUSTED') || String(err.message || '').includes('Quota exceeded'))) {
    return res.status(200).json({
      degraded: true,
      error: 'Firestore quota limit reached. System running in degraded mode.',
      players: [],
      matches: [],
      auditLogs: [],
      securityLogs: [],
      metrics: { activeMatches: 0, connectedPlayers: 0 }
    });
  }
  return next(err);
});


// --- ADMIN ROUTES SERVED DIRECTLY FROM SQLITE (0ms / ZERO FIRESTORE READS) ---
app.get('/api/admin/players', (req, res) => {
  try {
    let players = [];
    if (sqliteDb) {
      players = sqliteDb.prepare('SELECT * FROM players ORDER BY lastSeen DESC LIMIT 100').all();
    }
    return res.json({ players, source: 'sqlite_cache', degraded: false });
  } catch (e) {
    return res.json({ players: [], degraded: true });
  }
});

app.get('/api/admin/matches', (req, res) => {
  try {
    let matches = [];
    if (sqliteDb) {
      matches = sqliteDb.prepare('SELECT * FROM matches ORDER BY playedAt DESC LIMIT 50').all();
    }
    return res.json({ matches, source: 'sqlite_cache', degraded: false });
  } catch (e) {
    return res.json({ matches: [], degraded: true });
  }
});

app.get('/api/admin/audit', (req, res) => {
  try {
    let auditLogs = [];
    if (sqliteDb) {
      auditLogs = sqliteDb.prepare('SELECT * FROM audit_logs ORDER BY timestamp DESC LIMIT 50').all();
    }
    return res.json({ auditLogs, source: 'sqlite_cache', degraded: false });
  } catch (e) {
    return res.json({ auditLogs: [], degraded: true });
  }
});

app.get('/api/admin/security', (req, res) => {
  return res.json({ securityLogs: [], degraded: false });
});


// --- ADMIN VOICE BROADCAST ENDPOINT & SOCKET FANOUT ---
app.post('/api/admin/broadcast-voice', adminAuth, async (req, res, next) => {
  try {
    const { audioData, durationSec, priority } = req.body || {};
    if (!audioData) {
      return res.status(400).json({ error: 'Audio payload required' });
    }

    const payload = {
      id: `voice-announcement-${Date.now()}`,
      type: 'voice',
      audioUrl: audioData, // Base64 Data URI
      durationSec: durationSec || 0,
      priority: priority || 'urgent',
      message: 'Admin Voice Announcement',
      createdAt: new Date().toISOString()
    };

    // Fan out to all connected clients via Socket.io
    io.emit('admin_voice_broadcast', payload);
    io.emit('admin_broadcast', payload);

    await auditAdminAction(req.admin, 'broadcast_voice_announcement', 'all_players', {
      durationSec,
      priority
    });

    res.json({ success: true, payload });
  } catch (err) {
    next(err);
  }
});
