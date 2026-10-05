const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');

// ─── Config ───
const PORT = process.env.PORT || 3001;
const ADMIN_KEY = process.env.ADMIN_KEY || 'whot-admin-2024';
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

// ─── Helpers ───
function log(emoji, message) {
    console.log(`[${new Date().toLocaleTimeString()}] ${emoji} ${message}`);
}

function isNonEmptyString(value, maxLength = 100) {
    return typeof value === 'string' && value.trim().length > 0 && value.length <= maxLength;
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

async function verifyPlayerToken(idToken) {
    if (!auth) return null;
    if (!isNonEmptyString(idToken, 2000)) {
        throw new Error('Missing or invalid Firebase ID token');
    }
    return auth.verifyIdToken(idToken);
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

app.get('/api/rooms', (req, res) => {
    if (req.query.key !== ADMIN_KEY) {
        return res.status(401).json({ error: 'Unauthorized. Provide valid admin key.' });
    }

    const rooms = Object.values(activeGames).map((game) => ({
        roomId: game.roomId,
        players: game.players.map((p) => ({
            name: p.name,
            uid: p.uid,
            isHost: p.isHost,
        })),
        playerCount: game.players.length,
        state: game.state,
        createdAt: game.createdAt,
    }));

    res.json({ rooms, ...getServerStats() });
});

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

    // Join matchmaking
    socket.on('join_queue', async (playerData, callback) => {
        try {
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

            if (firebaseReady && playerData.idToken) {
                const decoded = await verifyPlayerToken(playerData.idToken);
                if (decoded) {
                    uid = decoded.uid;
                    email = decoded.email || '';
                    name = playerData.name || decoded.name || (email ? email.split('@')[0] : 'Player');
                    photoURL = isNonEmptyString(playerData.photoURL, 2048)
                        ? playerData.photoURL
                        : (decoded.picture || null);
                } else {
                    uid = playerData.uid ? playerData.uid.trim() : socket.id;
                    name = playerData.name ? playerData.name.trim() : 'Player';
                    photoURL = isNonEmptyString(playerData.photoURL, 2048) ? playerData.photoURL : null;
                }
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

            socket.playerData = { uid, name, email, photoURL };

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

                if (roomPlayers.length > MAX_PLAYERS_PER_ROOM) {
                    if (typeof callback === 'function') callback({ error: 'Room is full.' });
                    return;
                }

                activeGames[roomId] = {
                    roomId,
                    players: roomPlayers,
                    state: 'playing',
                    turnIndex: 0,
                    createdAt: new Date().toLocaleTimeString(),
                };

                io.to(roomId).emit('match_found', {
                    roomId,
                    players: roomPlayers,
                    startingPlayerIndex: 0,
                });

                log('🎮', `Match created: ${roomId} (${roomPlayers.map((p) => p.name).join(' vs ')})`);
                if (typeof callback === 'function') callback({ success: true, roomId });
            } else {
                queue.push({ socket, playerData: socket.playerData });
                socket.emit('waiting', { message: 'Searching for online players...' });
                log('⏳', `${socket.playerData.name} entered queue. Queue size: ${queue.length}`);
                if (typeof callback === 'function') callback({ success: true, queued: true });
            }
        } catch (err) {
            log('💥', `join_queue error: ${err.message}`);
            if (typeof callback === 'function') {
                callback({ error: err.message || 'Server error while joining queue.' });
            }
            socket.emit('action_error', { error: err.message || 'Auth/join failed.' });
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

    socket.on('play_card', (data) => {
        try {
            if (!validateRoomAction(data)) {
                socket.emit('action_error', { error: 'Invalid room or you are not in this game.' });
                return;
            }
            socket.to(data.roomId).emit('opponent_played_card', data);
        } catch (err) {
            log('💥', `play_card error: ${err.message}`);
        }
    });

    socket.on('draw_card', (data) => {
        try {
            if (!validateRoomAction(data)) {
                socket.emit('action_error', { error: 'Invalid room or you are not in this game.' });
                return;
            }
            socket.to(data.roomId).emit('opponent_drew_card', data);
        } catch (err) {
            log('💥', `draw_card error: ${err.message}`);
        }
    });

    socket.on('call_whot', (data) => {
        try {
            if (!validateRoomAction(data) || !isNonEmptyString(data.newShape, 20)) {
                socket.emit('action_error', { error: 'Invalid Whot call.' });
                return;
            }
            socket.to(data.roomId).emit('opponent_called_whot', data);
        } catch (err) {
            log('💥', `call_whot error: ${err.message}`);
        }
    });

    socket.on('voice_offer', (data) => {
        if (!validateRoomAction(data) || !data.sdp || !isNonEmptyString(data.sdp.sdp, 10000)) {
            socket.emit('action_error', { error: 'Invalid voice offer or room membership.' });
            return;
        }
        socket.to(data.roomId).emit('voice_offer', { roomId: data.roomId, sdp: data.sdp });
    });

    socket.on('voice_answer', (data) => {
        if (!validateRoomAction(data) || !data.sdp || !isNonEmptyString(data.sdp.sdp, 10000)) {
            socket.emit('action_error', { error: 'Invalid voice answer or room membership.' });
            return;
        }
        socket.to(data.roomId).emit('voice_answer', { roomId: data.roomId, sdp: data.sdp });
    });

    socket.on('ice_candidate', (data) => {
        if (!validateRoomAction(data) || !data.candidate || typeof data.candidate.candidate !== 'string' || data.candidate.candidate.length > 4096) {
            socket.emit('action_error', { error: 'Invalid ICE candidate or room membership.' });
            return;
        }
        socket.to(data.roomId).emit('ice_candidate', { roomId: data.roomId, candidate: data.candidate });
    });

    socket.on('game_over', async (data) => {
        try {
            if (!validateRoomAction(data)) return;

            const game = activeGames[data.roomId];
            const winnerUid = data.winnerUid || data.winnerId;
            if (game.ending) return;
            if (!isNonEmptyString(winnerUid, 200) || !game.players.some((player) => player.uid === winnerUid)) {
                socket.emit('action_error', { error: 'Winner must be a player in this room.' });
                return;
            }
            game.ending = true;

            io.to(data.roomId).emit('game_ended', {
                roomId: data.roomId,
                winnerUid,
            });

            if (db && FieldValue && game) {
                try {
                    const playersByUid = new Map(
                        game.players
                            .filter((player) => isNonEmptyString(player.uid, 200))
                            .map((player) => [player.uid, player])
                    );
                    const players = [...playersByUid.values()];
                    const userRefs = players.map((player) => db.collection('users').doc(player.uid));
                    const matchRef = db.collection('matches').doc();
                    const updatedAt = new Date().toISOString();

                    await db.runTransaction(async (transaction) => {
                        const userSnapshots = await Promise.all(userRefs.map((userRef) => transaction.get(userRef)));
                        transaction.set(matchRef, {
                            roomId: data.roomId,
                            players: game.players.map((player) => ({
                                uid: player.uid,
                                name: player.name,
                                ...(player.photoURL ? { photoURL: player.photoURL } : {}),
                            })),
                            winnerUid,
                            playedAt: FieldValue.serverTimestamp(),
                        });

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

    socket.on('get_admin_data', (data, callback) => {
        try {
            if (data && data.adminKey && data.adminKey !== ADMIN_KEY) {
                if (typeof callback === 'function') callback({ error: 'Unauthorized.' });
                return;
            }

            const rooms = Object.values(activeGames).map((game) => ({
                roomId: game.roomId,
                playerCount: game.players.length,
                playerNames: game.players.map((p) => p.name),
                state: game.state,
                createdAt: game.createdAt,
            }));

            const payload = { activeRooms: rooms, ...getServerStats() };
            socket.emit('admin_data_response', payload);
            if (typeof callback === 'function') callback(payload);
        } catch (err) {
            log('💥', `get_admin_data error: ${err.message}`);
        }
    });

    socket.on('disconnect', (reason) => {
        try {
            const qIdx = queue.findIndex((q) => q.socket.id === socket.id);
            if (qIdx !== -1) {
                queue.splice(qIdx, 1);
                log('🚪', `${socket.playerData?.name || socket.id} removed from queue (disconnect).`);
            }

            for (const [roomId, game] of Object.entries(activeGames)) {
                const player = game.players.find((p) => p.socketId === socket.id);
                if (player) {
                    socket.to(roomId).emit('opponent_disconnected', {
                        message: `${player.name} disconnected.`,
                        roomId,
                    });
                    delete activeGames[roomId];
                    log('❌', `Room ${roomId} closed (${player.name} disconnected: ${reason}).`);
                    break;
                }
            }

            log('🔴', `Player disconnected: ${socket.id} (${reason})`);
        } catch (err) {
            log('💥', `disconnect cleanup error: ${err.message}`);
        }
    });

    socket.onAny((event) => {
        const knownEvents = [
            'join_queue',
            'leave_queue',
            'play_card',
            'draw_card',
            'call_whot',
            'voice_offer',
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
server.listen(PORT, () => {
    log('🚀', `Whot Backend Server running on http://localhost:${PORT}`);
    log(
        'ℹ️',
        `REST: /  /health  /api/stats  /api/rooms?key=${ADMIN_KEY === 'whot-admin-2024' ? 'whot-admin-2024' : '<your-key>'
        }`
    );

    startSelfPing();
});
