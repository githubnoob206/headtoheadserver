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

function broadcastBombState(roomId, room, accepted = true) {
  const now = Date.now();
  io.to(roomId).emit('bombState', {
    roomId,
    holder: room.bombHolder,
    seq: room.bombSeq,
    accepted,
    passLockMs: Math.max(0, room.nextPassAt - now),
    bombTimeMs: Math.max(0, room.bombEndsAt - now)
  });
}

function transferBomb(roomId, room, now = Date.now()) {
  if (
    room.paused ||
    room.contactLatched ||
    room.bombHolder < 0 ||
    now < room.nextPassAt
  ) {
    return false;
  }

  room.contactLatched = true;
  room.bombHolder = 1 - room.bombHolder;
  room.bombSeq++;
  room.nextPassAt = now + 1000;
  room.bombEndsAt = now + 10000;
  broadcastBombState(roomId, room, true);
  return true;
}

function checkReportedContact(roomId, room) {
  const [a, b] = room.playerStates;
  if (!a || !b) return;

  const xTouch = Math.abs(a.x - b.x) <= 48;
  const yTouch = Math.abs(a.h - b.h) <= 52;

  if (!xTouch || !yTouch) {
    room.contactLatched = false;
  } else if (!room.contactLatched) {
    transferBomb(roomId, room);
  }
}

function scheduleNextRound(roomId, room) {
  if (room.roundTimer) clearTimeout(room.roundTimer);

  const wait = Math.max(0, room.roundEndsAt - Date.now());
  room.roundTimer = setTimeout(() => {
    if (!rooms.has(roomId) || !room.started || room.players.length !== 2) {
      return;
    }

    if (room.paused) {
      room.roundTimer = setTimeout(() => scheduleNextRound(roomId, room), 100);
      return;
    }

    room.bombSeq++;
    room.bombHolder = (room.scores[0] + room.scores[1]) % 2;
    room.contactLatched = false;
    room.nextPassAt = Date.now() + 1000;
    room.bombEndsAt = Date.now() + 10000;
    room.roundEndsAt = 0;
    room.playerStates = [null, null];

    io.to(roomId).emit('newRound', {
      roomId,
      scores: room.scores,
      bombHolder: room.bombHolder,
      seq: room.bombSeq,
      passLockMs: 1000,
      bombTimeMs: 10000
    });
  }, wait);
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
      contactLatched: false,
      scores: [0, 0],
      nextPassAt: 0,
      bombEndsAt: 0,
      roundTimer: null,
      roundEndsAt: 0,
      playerStates: [null, null],
      active: [true, true],
      paused: false,
      pausedAt: 0
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
    room.contactLatched = false;
    room.scores = [0, 0];
    room.playerStates = [null, null];
    room.active = [true, true];
    room.paused = false;
    room.pausedAt = 0;
    room.roundEndsAt = 0;
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

    if (!room || !room.started || room.paused || !ACTIONS.has(payload?.action)) {
      return;
    }

    socket.to(roomId).emit('playerInput', {
      roomId,
      action: payload.action,
      down: !!payload.down
    });
  });

  socket.on('playerState', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (!room || !room.started || room.paused) return;

    const player = room.players.find(p => p.socketId === socket.id);
    const source = payload?.state;

    if (
      !player ||
      !source ||
      !Number.isFinite(source.x) ||
      !Number.isFinite(source.h)
    ) {
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

    room.playerStates[player.playerIndex] = state;
    socket.to(roomId).emit('playerState', {
      roomId,
      playerIndex: player.playerIndex,
      state
    });
    checkReportedContact(roomId, room);
  });

  // Host-authoritative game: the host simulates, the guest only renders.
  // Relay the host's snapshots to the other player (volatile = fine to drop).
  socket.on('gameState', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (
      !room ||
      !room.started ||
      room.hostSocketId !== socket.id ||
      !payload ||
      typeof payload !== 'object'
    ) {
      return;
    }

    socket.to(roomId).volatile.emit('gameState', { ...payload, roomId });
  });

  socket.on('setActive', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);
    const player = room?.players.find(p => p.socketId === socket.id);

    if (!room || !room.started || !player) return;

    room.active[player.playerIndex] = !!payload?.active;
    const shouldPause = room.active.some(active => !active);

    if (shouldPause === room.paused) return;

    const now = Date.now();

    if (shouldPause) {
      room.paused = true;
      room.pausedAt = now;
    } else {
      const held = now - room.pausedAt;
      room.nextPassAt += held;
      room.bombEndsAt += held;

      if (room.roundEndsAt) {
        room.roundEndsAt += held;
        scheduleNextRound(roomId, room);
      }

      room.paused = false;
      room.pausedAt = 0;
    }

    io.to(roomId).emit('gamePaused', {
      roomId,
      paused: room.paused
    });
  });

  socket.on('bombPass', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (
      !room ||
      !room.started ||
      room.paused ||
      !room.players.some(player => player.socketId === socket.id)
    ) {
      return;
    }

    const now = Date.now();

    if (
      payload?.fromHolder !== room.bombHolder ||
      payload?.seq !== room.bombSeq ||
      now < room.nextPassAt ||
      room.contactLatched
    ) {
      socket.emit('bombState', {
        roomId,
        holder: room.bombHolder,
        seq: room.bombSeq,
        accepted: false,
        passLockMs: Math.max(0, room.nextPassAt - now),
        bombTimeMs: Math.max(0, room.bombEndsAt - now)
      });
      return;
    }

    transferBomb(roomId, room, now);
  });

  socket.on('bombExploded', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (
      !room ||
      !room.started ||
      room.paused ||
      payload?.holder !== room.bombHolder ||
      payload?.seq !== room.bombSeq
    ) {
      return;
    }

    if (Date.now() < room.bombEndsAt) return;

    room.scores[1 - room.bombHolder]++;
    room.bombHolder = -1;
    room.roundEndsAt = Date.now() + 1500;

    io.to(roomId).emit('roundEnded', {
      roomId,
      scores: room.scores
    });

    scheduleNextRound(roomId, room);
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
