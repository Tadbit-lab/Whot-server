const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const cors = require('cors');
const admin = require('firebase-admin');

// ─── Config ───
const PORT = process.env.PORT || 3001;
const ADMIN_KEY = process.env.ADMIN_KEY || 'whot-admin-2024'; // change in production
const MAX_PLAYERS_PER_ROOM = 6;

// ─── Firebase Admin Init ───
let firebaseReady = false;
try {
    let serviceAccount;

    if (process.env.FIREBASE_SERVICE_ACCOUNT) {
        // Production (Railway / Render / etc.)
        serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT);
    } else {
        // Local development
        // Make sure serviceAccountKey.json exists in whot-server/
        serviceAccount = require('./serviceAccountKey.json');
    }

    admin.initializeApp({
        credential: admin.credential.cert(serviceAccount),
    });

    firebaseReady = true;
    console.log('🔥 Firebase Admin SDK initialized');
} catch (err) {
    console.warn('⚠️ Firebase Admin not initialized:', err.message);
    console.warn('   Server will still run, but token verification & Firestore writes are disabled.');
}

const db = firebaseReady ? admin.firestore() : null;
const auth = firebaseReady ? admin.auth() : null;

// ─── Express + Socket.io ───
const app = express();
app.use(cors());
app.use(express.json());

const server = http.createServer(app);
const io = new Server(server, {
    cors: {
        origin: '*', // later: put your Vercel URL here
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
    if (!auth) {
        // Firebase not configured — allow local testing with payload fields
        return null;
    }
    if (!isNonEmptyString(idToken, 2000)) {
        throw new Error('Missing or invalid Firebase ID token');
    }
    return auth.verifyIdToken(idToken);
}

// ─── REST API Routes ───
app.get('/', (req, res) => {
    res.json({
        name: 'Whot Game Server',
        version: '1.2.0',
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

// 404
app.use((req, res) => {
    res.status(404).json({ error: 'Route not found', path: req.originalUrl });
});

// Express error handler
app.use((err, req, res, next) => {
    log('💥', `Express error: ${err.message}`);
    res.status(500).json({ error: 'Internal server error' });
});

// ─── Socket.io ───
io.on('connection', (socket) => {
    log('🟢', `Player connected: ${socket.id}`);

    // ── Join matchmaking ──
    // Preferred payload:
    // { idToken, name?, uid? }
    // If Firebase is enabled, idToken is verified and uid/name come from Firebase.
    socket.on('join_queue', async (playerData, callback) => {
        try {
            if (!playerData || typeof playerData !== 'object') {
                const errMsg = 'Invalid player data.';
                if (typeof callback === 'function') callback({ error: errMsg });
                return;
            }

            // Prevent double queue
            if (queue.some((q) => q.socket.id === socket.id)) {
                if (typeof callback === 'function') callback({ error: 'You are already in the queue.' });
                return;
            }

            // Prevent join while already in a room
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

            // Verify Firebase token if available
            if (firebaseReady && playerData.idToken) {
                const decoded = await verifyPlayerToken(playerData.idToken);
                uid = decoded.uid;
                email = decoded.email || '';
                name =
                    decoded.name ||
                    playerData.name ||
                    (email ? email.split('@')[0] : 'Player');
            } else {
                // Local/dev fallback (no Firebase token)
                if (!isNonEmptyString(playerData.name) || !isNonEmptyString(playerData.uid)) {
                    const errMsg = 'Name and uid are required when Firebase token is not provided.';
                    if (typeof callback === 'function') callback({ error: errMsg });
                    return;
                }
                uid = playerData.uid.trim();
                name = playerData.name.trim();
            }

            socket.playerData = { uid, name, email };

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

                // Optional: cap room size later for 2–6 player lobbies
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

    // Leave queue
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

    // Game over + optional Firestore save
    socket.on('game_over', async (data) => {
        try {
            if (!validateRoomAction(data)) return;

            const game = activeGames[data.roomId];

            io.to(data.roomId).emit('game_ended', {
                roomId: data.roomId,
                winnerUid: data.winnerUid,
            });

            // Save match history if Firebase is ready
            if (db && game) {
                try {
                    await db.collection('matches').add({
                        roomId: data.roomId,
                        players: game.players.map((p) => ({
                            uid: p.uid,
                            name: p.name,
                        })),
                        winnerUid: data.winnerUid || null,
                        playedAt: admin.firestore.FieldValue.serverTimestamp(),
                    });
                    log('💾', `Match saved to Firestore: ${data.roomId}`);
                } catch (saveErr) {
                    log('⚠️', `Failed to save match: ${saveErr.message}`);
                }
            }

            delete activeGames[data.roomId];
            log('🏁', `Game ended in ${data.roomId}. Room closed.`);
        } catch (err) {
            log('💥', `game_over error: ${err.message}`);
        }
    });

    // Admin data over socket
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

    // Disconnect cleanup
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
            'game_over',
            'get_admin_data',
            'disconnect',
        ];
        if (!knownEvents.includes(event)) {
            log('⚠️', `Unknown event "${event}" from ${socket.id}`);
        }
    });
});

// ─── Graceful shutdown ───
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

server.listen(PORT, () => {
    log('🚀', `Whot Backend Server running on http://localhost:${PORT}`);
    log(
        'ℹ️',
        `REST: /  /health  /api/stats  /api/rooms?key=${ADMIN_KEY === 'whot-admin-2024' ? 'whot-admin-2024' : '<your-key>'
        }`
    );
});