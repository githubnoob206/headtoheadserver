'use strict';

const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*', methods: ['GET', 'POST'] },
  transports: ['websocket', 'polling'],
  pingInterval: 25000,
  pingTimeout: 20000
});

const PORT = Number(process.env.PORT) || 3000;
const rooms = new Map();
const ACTIONS = new Set(['left', 'right', 'jump']);
const MAX_LOBBIES = 100;

app.disable('x-powered-by');
app.get('/health', (_req, res) =>
  res.json({ ok: true, openLobbies: publicLobbyList().length })
);
app.get('*', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));

function cleanName(value) {
  const name = String(value || 'Player')
    .replace(/[<>\u0000-\u001f]/g, '')
    .trim()
    .slice(0, 20);
  return name || 'Player';
}

function publicLobbyList() {
  return [...rooms.values()]
    .filter(room => !room.started && room.players.length === 1)
    .map(room => ({
      roomId: room.id,
      name: room.players[0].name,
      count: 1
    }));
}

function broadcastLobbies() {
  io.emit('lobbyList', publicLobbyList());
}

function sendRoomChanged(room) {
  io.to(room.id).emit('lobbyChanged', {
    roomId: room.id,
    count: room.players.length,
    started: room.started
  });
  broadcastLobbies();
}

function removePlayer(socket, roomId, notify = true) {
  const room = rooms.get(roomId);
  if (!room) return;

  const idx = room.players.findIndex(player => player.socketId === socket.id);
  if (idx < 0) return;

  room.players.splice(idx, 1);
  socket.leave(roomId);
  socket.data.roomId = null;

  if (notify) io.to(roomId).emit('opponentLeft');

  if (room.players.length === 0) {
    if (room.roundTimer) clearTimeout(room.roundTimer);
    rooms.delete(roomId);
    io.to(roomId).emit('lobbyClosed');
  } else {
    // A remaining player can keep the public lobby open and become its host.
    room.hostSocketId = room.players[0].socketId;
    room.started = false;
    if (room.roundTimer) clearTimeout(room.roundTimer);
    room.players[0].playerIndex = 0;
    io.to(room.players[0].socketId).emit('hostChanged');
    sendRoomChanged(room);
  }

  broadcastLobbies();
}

io.on('connection', socket => {
  socket.data.roomId = null;
  socket.emit('lobbyList', publicLobbyList());

  socket.on('listLobbies', () => {
    socket.emit('lobbyList', publicLobbyList());
  });

  socket.on('createLobby', payload => {
    if (socket.data.roomId) {
      removePlayer(socket, socket.data.roomId, false);
    }

    if (rooms.size >= MAX_LOBBIES) {
      socket.emit('lobbyError', {
        message: 'The lobby list is full right now. Try again in a moment.'
      });
      return;
    }

    const id = `${Date.now().toString(36)}-${Math.random()
      .toString(36)
      .slice(2, 9)}`;

    const room = {
      id,
      hostSocketId: socket.id,
      players: [{
        socketId: socket.id,
        name: cleanName(payload?.name),
        playerIndex: 0
      }],
      started: false,
      bombHolder: null,
      bombSeq: 0,
      scores: [0, 0],
      nextPassAt: 0,
      bombEndsAt: 0,
      roundTimer: null
    };

    rooms.set(id, room);
    socket.join(id);
    socket.data.roomId = id;
    socket.emit('lobbyCreated', {
      roomId: id,
      playerIndex: 0,
      host: true,
      count: 1
    });
    sendRoomChanged(room);
  });

  socket.on('joinLobby', payload => {
    const room = rooms.get(String(payload?.roomId || ''));

    if (!room || room.started || room.players.length !== 1) {
      socket.emit('lobbyError', {
        message: 'That lobby is no longer open. Refresh the list and try another.'
      });
      broadcastLobbies();
      return;
    }

    if (socket.data.roomId) {
      removePlayer(socket, socket.data.roomId, false);
    }

    room.players.push({
      socketId: socket.id,
      name: cleanName(payload?.name),
      playerIndex: 1
    });

    socket.join(room.id);
    socket.data.roomId = room.id;
    socket.emit('lobbyJoined', {
      roomId: room.id,
      playerIndex: 1,
      host: false,
      count: 2
    });
    sendRoomChanged(room);
  });

  socket.on('startGame', payload => {
    const room = rooms.get(String(payload?.roomId || ''));

    if (
      !room ||
      room.hostSocketId !== socket.id ||
      room.players.length !== 2 ||
      room.started
    ) {
      return;
    }

    room.started = true;
    room.bombHolder = Number(payload?.bombHolder) === 1 ? 1 : 0;
    room.bombSeq = 0;
    room.scores = [0, 0];
    room.nextPassAt = Date.now() + 1000;
    room.bombEndsAt = Date.now() + 10000;

    io.to(room.id).emit('gameStarted', {
      roomId: room.id,
      bombHolder: room.bombHolder,
      seq: room.bombSeq,
      passLockMs: 1000,
      bombTimeMs: 10000
    });
    broadcastLobbies();
  });

  socket.on('playerInput', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (!room || !room.started || !ACTIONS.has(payload?.action)) return;

    socket.to(roomId).emit('playerInput', {
      roomId,
      action: payload.action,
      down: !!payload.down
    });
  });

  socket.on('playerState', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (!room || !room.started) return;

    const player = room.players.find(p => p.socketId === socket.id);
    const source = payload?.state;

    if (!player || !source ||
        !Number.isFinite(source.x) ||
        !Number.isFinite(source.h)) {
      return;
    }

    const state = {};
    for (const key of [
      'x', 'h', 'vx', 'vy', 'angle', 'air',
      'spin', 'jumpIn', 'deadIn', 'shieldIn'
    ]) {
      if (Number.isFinite(source[key])) {
        state[key] = source[key];
      }
    }

    socket.to(roomId).emit('playerState', {
      roomId,
      playerIndex: player.playerIndex,
      state
    });
  });

  socket.on('bombPass', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (!room || !room.started ||
        !room.players.some(p => p.socketId === socket.id)) {
      return;
    }

    const now = Date.now();

    if (
      payload?.fromHolder !== room.bombHolder ||
      payload?.seq !== room.bombSeq ||
      now < room.nextPassAt
    ) {
      socket.emit('bombState', {
        roomId,
        holder: room.bombHolder,
        seq: room.bombSeq,
        passLockMs: Math.max(0, room.nextPassAt - now),
        bombTimeMs: Math.max(0, room.bombEndsAt - now)
      });
      return;
    }

    room.bombHolder = 1 - room.bombHolder;
    room.bombSeq++;
    room.nextPassAt = now + 1000;
    room.bombEndsAt = now + 10000;

    io.to(roomId).emit('bombState', {
      roomId,
      holder: room.bombHolder,
      seq: room.bombSeq,
      passLockMs: 1000,
      bombTimeMs: 10000
    });
  });

  socket.on('bombExploded', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (
      !room ||
      !room.started ||
      payload?.holder !== room.bombHolder ||
      payload?.seq !== room.bombSeq
    ) {
      return;
    }

    if (Date.now() < room.bombEndsAt) return;

    room.scores[1 - room.bombHolder]++;
    room.bombHolder = -1;

    io.to(roomId).emit('roundEnded', {
      roomId,
      scores: room.scores
    });

    if (room.roundTimer) clearTimeout(room.roundTimer);

    room.roundTimer = setTimeout(() => {
      if (!rooms.has(roomId) || !room.started || room.players.length !== 2) {
        return;
      }

      room.bombSeq++;
      room.bombHolder = (room.scores[0] + room.scores[1]) % 2;
      room.nextPassAt = Date.now() + 1000;
      room.bombEndsAt = Date.now() + 10000;

      io.to(roomId).emit('newRound', {
        roomId,
        scores: room.scores,
        bombHolder: room.bombHolder,
        seq: room.bombSeq,
        passLockMs: 1000,
        bombTimeMs: 10000
      });
    }, 1500);
  });

  socket.on('leaveLobby', payload => {
    const roomId = String(payload?.roomId || socket.data.roomId || '');
    if (socket.data.roomId === roomId) {
      removePlayer(socket, roomId);
    }
  });

  socket.on('disconnect', () => {
    if (socket.data.roomId) {
      removePlayer(socket, socket.data.roomId);
    }
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Head to Head public lobby server listening on ${PORT}`);
});
