const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

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
app.use(cors());
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
const securityLogs = [];
const MAX_SECURITY_LOGS = 100;
const metrics = {
    gamesStarted: 0,
    gamesCompleted: 0,
    forfeits: 0,
    actionErrors: 0,
    voiceEventsRelayed: 0,
    lastGameAt: null,
};
let matchmakingPaused = false;
let featureFlags = {
    maintenanceMode: false,
    voiceEnabled: true,
    rankedEnabled: true,
    maxPlayers: MAX_PLAYERS_PER_ROOM,
    minStartingCards: 4,
    maxStartingCards: 8,
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
        ip,
        severity,
        details: String(details).slice(0, 300),
    };
    securityLogs.unshift(event);
    if (securityLogs.length > MAX_SECURITY_LOGS) securityLogs.length = MAX_SECURITY_LOGS;
    log('SECURITY', `${severity} ${type} uid=${uid || 'unknown'} room=${roomId || 'none'} ${event.details}`);
    return event;
}

function clientIp(socket) {
    return String(socket.handshake?.address || socket.conn?.remoteAddress || 'unknown').replace(/^::ffff:/, '');
}

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
        })),
        playerCount: game.players.length,
        settings: game.settings,
        turnIndex: game.turnIndex ?? 0,
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
    if (!auth || !ADMIN_EMAIL) throw new Error('Admin authentication is unavailable.');
    const decoded = await verifyPlayerToken(token);
    if (!decoded?.email || decoded.email.toLowerCase() !== ADMIN_EMAIL) {
        throw new Error('Unauthorized.');
    }
    return { uid: decoded.uid, email: decoded.email };
}

async function adminAuth(req, res, next) {
    try {
        const authorization = String(req.headers.authorization || '');
        const token = authorization.startsWith('Bearer ') ? authorization.slice(7) : '';
        req.admin = await requireAdminToken(token);
        next();
    } catch (error) {
        res.status(401).json({ error: error.message || 'Unauthorized.' });
    }
}

function emitActionError(socket, message, event = 'action_error') {
    metrics.actionErrors += 1;
    socket.emit(event, { error: message });
}

async function loadFeatureFlags() {
    if (!db) return;
    try {
        const snapshot = await db.collection('adminConfig').doc('flags').get();
        if (snapshot.exists) featureFlags = { ...featureFlags, ...snapshot.data() };
    } catch (error) {
        log('⚠️', `Feature flag load failed: ${error.message}`);
    }
}

function emitRoomClosed(game, message) {
    io.to(game.roomId).emit('room_closed', { roomId: game.roomId, message });
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
        .filter((player) => player.uid).map((player) => [player.uid, player])).values()];
    const userRefs = players.map((player) => db.collection('users').doc(player.uid));
    await db.runTransaction(async (transaction) => {
        const snapshots = await Promise.all(userRefs.map((ref) => transaction.get(ref)));
        players.forEach((player, index) => {
            const current = snapshots[index].exists ? snapshots[index].data() : {};
            const won = player.uid === winnerUid;
            const streak = typeof current.winStreak === 'number' ? current.winStreak : 0;
            const best = typeof current.bestWinStreak === 'number' ? current.bestWinStreak : 0;
            transaction.set(userRefs[index], {
                wins: FieldValue.increment(won ? 1 : 0),
                losses: FieldValue.increment(winnerUid && !won ? 1 : 0),
                matchesPlayed: FieldValue.increment(1),
                winStreak: won ? FieldValue.increment(1) : 0,
                bestWinStreak: won ? Math.max(best, streak + 1) : best,
                updatedAt: new Date().toISOString(),
                lastPlayedAt: new Date().toISOString(),
            }, { merge: true });
        });
    });
}

async function adminMetrics() {
    const rooms = Object.values(activeGames).map(publicRoomSummary);
    let gamesLast24h = 0;
    let totalMatches = metrics.gamesCompleted;
    let forfeitCount = metrics.forfeits;
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
        }
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
    if (featureFlags.maintenanceMode) throw new Error('The game is temporarily unavailable for maintenance.');
    if (!db || !uid) return;
    const snapshot = await db.collection('users').doc(uid).get();
    if (snapshot.exists && isPlayerBanned(snapshot.data())) throw new Error('Account suspended for policy violations.');
}

const DEFAULT_SETTINGS = { minPlayers: 2, maxPlayers: 6, startingHandSize: 6, decks: 1 };
const ADMIN_EMAIL = String(process.env.VITE_ADMIN_EMAIL || process.env.ADMIN_EMAIL || '').toLowerCase();
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
function publicRoom(room) {
    syncHostState(room);
    return { roomId: room.roomId, hostUid: room.hostUid, players: room.players.map(({ uid, name, photoURL, isHost, isAdmin, calledLastCard }) => ({ uid, name, photoURL, isHost, isAdmin, calledLastCard: Boolean(calledLastCard) })),
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
    };
}
function broadcastGameState(room) {
    io.to(room.roomId).emit('game_state', gameSnapshot(room));
    room.players.forEach((player) => io.to(player.socketId).emit('hand_update', { roomId: room.roomId, hand: room.handsByUid[player.uid] || [] }));
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
        const allowed = ['maintenanceMode', 'voiceEnabled', 'rankedEnabled', 'maxPlayers', 'minStartingCards', 'maxStartingCards'];
        const nextFlags = { ...featureFlags };
        for (const key of allowed) {
            if (Object.prototype.hasOwnProperty.call(req.body || {}, key)) nextFlags[key] = req.body[key];
        }
        if (typeof nextFlags.maintenanceMode !== 'boolean' || typeof nextFlags.voiceEnabled !== 'boolean'
            || typeof nextFlags.rankedEnabled !== 'boolean' || !Number.isInteger(nextFlags.maxPlayers)
            || nextFlags.maxPlayers < 2 || nextFlags.maxPlayers > MAX_PLAYERS_PER_ROOM
            || !Number.isInteger(nextFlags.minStartingCards) || nextFlags.minStartingCards < 4
            || !Number.isInteger(nextFlags.maxStartingCards) || nextFlags.maxStartingCards > 8
            || nextFlags.minStartingCards > nextFlags.maxStartingCards) {
            return res.status(400).json({ error: 'Invalid feature flag values.' });
        }
        featureFlags = nextFlags;
        if (db) await db.collection('adminConfig').doc('flags').set(featureFlags, { merge: true });
        await auditAdminAction(req.admin, 'update_flags', 'global', featureFlags);
        io.emit('feature_flags_updated', featureFlags);
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

app.post('/api/admin/announcement', adminAuth, async (req, res, next) => {
    try {
        const message = String(req.body?.message || '').trim();
        if (!isNonEmptyString(message, 500)) return res.status(400).json({ error: 'Announcement must be 1–500 characters.' });
        const announcement = { message, sentAt: new Date().toISOString() };
        io.emit('admin_announcement', announcement);
        await auditAdminAction(req.admin, 'broadcast_announcement', 'all_players', { message });
        res.json({ announcement });
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
    socket.data.requestStartedAt = Date.now();
    socket.data.requestCount = 0;
    socket.data.throttledUntil = 0;
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
        await assertPlayerAllowed(uid);
        const email = decoded?.email || '';
        const name = playerData.name || decoded?.name || (email ? email.split('@')[0] : 'Player');
        const photoURL = isNonEmptyString(playerData.photoURL, 2048) ? playerData.photoURL : (decoded?.picture || null);
        const isAdmin = Boolean(ADMIN_EMAIL && email.toLowerCase() === ADMIN_EMAIL);
        const ip = clientIp(socket);
        socket.playerData = { uid, name, email, photoURL, isAdmin, ip };
        return { uid, name, email, photoURL, isAdmin, isHost: false, socketId: socket.id, ip };
    }

    socket.on('create_room', async (playerData, callback) => {
        try {
            if (featureFlags.maintenanceMode) throw new Error('Online play is temporarily unavailable for maintenance.');
            const player = await resolveRoomPlayer(playerData);
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
            if (featureFlags.maintenanceMode) throw new Error('Online play is temporarily unavailable for maintenance.');
            const player = await resolveRoomPlayer(playerData);
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
        if (featureFlags.maintenanceMode) {
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
            if (featureFlags.maintenanceMode) throw new Error('Online play is temporarily unavailable for maintenance.');
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
            const ip = clientIp(socket);
            socket.playerData = { uid, name, email, photoURL, isAdmin, ip };

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
                    createdAt: new Date().toLocaleTimeString(),
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
        if (!actor || game.state !== 'playing' || game.status !== 'playing') return null;
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
                    const playersByUid = new Map(
                        game.players
                            .filter((player) => isNonEmptyString(player.uid, 200))
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
            await requireAdminToken(data?.idToken);
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
            await requireAdminToken(data?.idToken);
            const logs = [...securityLogs];
            socket.emit('security_logs_response', { logs });
            if (typeof callback === 'function') callback({ logs });
        } catch (error) {
            const response = { error: error.message || 'Unauthorized.' };
            socket.emit('security_logs_response', response);
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

    socket.on('disconnect', async (reason) => {
        try {
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
