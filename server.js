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
app.get('/health', (_req, res) => res.json({ ok: true, openLobbies: publicLobbyList().length }));
app.get('*', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));

function cleanName(value) {
  const name = String(value || 'Player').replace(/[<>\u0000-\u001f]/g, '').trim().slice(0, 20);
  return name || 'Player';
}

function publicLobbyList() {
  return [...rooms.values()]
    .filter(room => !room.started && room.players.length === 1)
    .map(room => ({ roomId: room.id, name: room.players[0].name, count: 1 }));
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
    rooms.delete(roomId);
    io.to(roomId).emit('lobbyClosed');
  } else {
    // A remaining player can keep the public lobby open and become its host.
    room.hostSocketId = room.players[0].socketId;
    room.started = false;
    room.players[0].playerIndex = 0;
    io.to(room.players[0].socketId).emit('hostChanged');
    sendRoomChanged(room);
  }

  broadcastLobbies();
}

io.on('connection', socket => {
  socket.data.roomId = null;
  socket.emit('lobbyList', publicLobbyList());

  socket.on('listLobbies', () => socket.emit('lobbyList', publicLobbyList()));

  socket.on('createLobby', payload => {
    if (socket.data.roomId) removePlayer(socket, socket.data.roomId, false);

    if (rooms.size >= MAX_LOBBIES) {
      socket.emit('lobbyError', {
        message: 'The lobby list is full right now. Try again in a moment.'
      });
      return;
    }

    const id = `${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 9)}`;
    const room = {
      id,
      hostSocketId: socket.id,
      players: [{
        socketId: socket.id,
        name: cleanName(payload?.name),
        playerIndex: 0
      }],
      started: false
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

    if (socket.data.roomId) removePlayer(socket, socket.data.roomId, false);

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

    if (!room || room.hostSocketId !== socket.id || room.players.length !== 2 || room.started) {
      return;
    }

    room.started = true;
    const holder = Number(payload?.bombHolder) === 1 ? 1 : 0;
    io.to(room.id).emit('gameStarted', {
      roomId: room.id,
      bombHolder: holder
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

  socket.on('gameState', payload => {
    const roomId = socket.data.roomId;
    const room = roomId && rooms.get(roomId);

    if (!room || !room.started || room.hostSocketId !== socket.id) return;
    if (!Number.isFinite(payload?.time) ||
        !Array.isArray(payload?.cubes) ||
        payload.cubes.length !== 2) {
      return;
    }

    socket.to(roomId).emit('gameState', {
      roomId,
      time: payload.time,
      scores: Array.isArray(payload.scores) ? payload.scores.slice(0, 2) : [0, 0],
      bombHolder: payload.bombHolder,
      bombTime: payload.bombTime,
      passLockUntil: payload.passLockUntil,
      bombRoundEnd: payload.bombRoundEnd,
      cubes: payload.cubes
    });
  });

  socket.on('leaveLobby', payload => {
    const roomId = String(payload?.roomId || socket.data.roomId || '');
    if (socket.data.roomId === roomId) removePlayer(socket, roomId);
  });

  socket.on('disconnect', () => {
    if (socket.data.roomId) removePlayer(socket, socket.data.roomId);
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`Head to Head public lobby server listening on ${PORT}`);
});
