const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, { maxHttpBufferSize: 40e6 });

let waitingQueue = [];
let activeRooms = {};
let reportedCases = [];
const OWNER_KEY = "1234";

function hasLinksOrPhone(text) {
  return /(https?:\/\/[^\s]+)|(www\.[^\s]+)|(\.[a-z]{2,}\b)|(\b\d{10}\b)/gi.test(text);
}

app.use(express.static(path.join(__dirname, 'public')));

io.on('connection', (socket) => {
  io.emit('online_count', io.engine.clientsCount);

  socket.on('find_match', (userData) => {
    waitingQueue = waitingQueue.filter(s => s.id !== socket.id);
    if (socket.roomId && activeRooms[socket.roomId]) {
      socket.to(socket.roomId).emit('peer_skipped');
      delete activeRooms[socket.roomId];
      socket.leave(socket.roomId);
      socket.roomId = null;
    }

    socket.userData = {
      id: socket.id,
      isGuest: userData.isGuest,
      gender: userData.gender || 'Guest',
      name: userData.name || 'Stranger',
      age: userData.age || '-',
      place: userData.place || '-',
      dp: userData.dp || 'https://cdn-icons-png.flaticon.com/512/149/149071.png',
      posts: userData.posts || [],
      reels: userData.reels || [],
      song: userData.song || null,
      interests: (userData.interests || []).map(t => t.toLowerCase().trim()).filter(Boolean)
    };

    let idx = waitingQueue.findIndex(p => p.id !== socket.id && p.userData.interests.some(tag => socket.userData.interests.includes(tag)));
    if (idx === -1) idx = waitingQueue.findIndex(p => p.id !== socket.id);

    if (idx !== -1) {
      const peer = waitingQueue.splice(idx, 1)[0];
      const roomId = 'room_' + socket.id + '_' + peer.id;
      socket.join(roomId);
      peer.join(roomId);
      const commonTags = socket.userData.interests.filter(t => peer.userData.interests.includes(t));
      activeRooms[roomId] = { users: [socket.id, peer.id], isFriends: false, votes: new Set(), messages: [] };
      socket.roomId = roomId;
      peer.roomId = roomId;

      socket.emit('matched', { peerGender: peer.userData.gender, peerGuest: peer.userData.isGuest, commonTags });
      peer.emit('matched', { peerGender: socket.userData.gender, peerGuest: socket.userData.isGuest, commonTags });
    } else {
      waitingQueue.push(socket);
      socket.emit('waiting');
    }
  });

  socket.on('update_profile', (updatedData) => {
    if (socket.userData) {
      socket.userData.posts = updatedData.posts || socket.userData.posts;
      socket.userData.reels = updatedData.reels || socket.userData.reels;
      socket.userData.song = updatedData.song || socket.userData.song;
    }
  });

  socket.on('like_post', ({ peerId, postIdx }) => {
    const peerSocket = io.sockets.sockets.get(peerId);
    if (peerSocket && peerSocket.userData && peerSocket.userData.posts[postIdx]) {
      peerSocket.userData.posts[postIdx].likes++;
      if (peerSocket.roomId) {
        io.to(peerSocket.roomId).emit('profile_updated_peer', peerSocket.userData);
      }
    }
  });

  socket.on('comment_post', ({ peerId, postIdx, text, userName }) => {
    const peerSocket = io.sockets.sockets.get(peerId);
    if (peerSocket && peerSocket.userData && peerSocket.userData.posts[postIdx]) {
      peerSocket.userData.posts[postIdx].comments.push({ user: userName, text });
      if (peerSocket.roomId) {
        io.to(peerSocket.roomId).emit('profile_updated_peer', peerSocket.userData);
      }
    }
  });

  socket.on('like_reel', ({ peerId, reelIdx }) => {
    const peerSocket = io.sockets.sockets.get(peerId);
    if (peerSocket && peerSocket.userData && peerSocket.userData.reels[reelIdx]) {
      peerSocket.userData.reels[reelIdx].likes++;
      if (peerSocket.roomId) {
        io.to(peerSocket.roomId).emit('profile_updated_peer', peerSocket.userData);
      }
    }
  });

  socket.on('comment_reel', ({ peerId, reelIdx, text, userName }) => {
    const peerSocket = io.sockets.sockets.get(peerId);
    if (peerSocket && peerSocket.userData && peerSocket.userData.reels[reelIdx]) {
      peerSocket.userData.reels[reelIdx].comments.push({ user: userName, text });
      if (peerSocket.roomId) {
        io.to(peerSocket.roomId).emit('profile_updated_peer', peerSocket.userData);
      }
    }
  });

  // Typing Indicator Handlers
  socket.on('typing', () => {
    if (socket.roomId) {
      socket.to(socket.roomId).emit('display_typing');
    }
  });

  socket.on('stop_typing', () => {
    if (socket.roomId) {
      socket.to(socket.roomId).emit('hide_typing');
    }
  });

  // WebRTC Signaling Handlers
  socket.on('call_offer', ({ offer, target, video }) => {
    socket.to(target).emit('incoming_call', { offer, callerId: socket.id, video: video });
  });

  socket.on('call_answer', ({ answer, target }) => {
    socket.to(target).emit('call_answered', { answer });
  });

  socket.on('ice_candidate', ({ candidate, target }) => {
    socket.to(target).emit('ice_candidate', { candidate });
  });

  socket.on('end_call', ({ target }) => {
    if (target) socket.to(target).emit('call_ended');
  });

  socket.on('send_message', ({ text, media }) => {
    const room = activeRooms[socket.roomId];
    if (!room) return;
    if (!room.isFriends) {
      if (media && !media.includes('giphy')) return socket.emit('sys_msg', '🔒 Become friends to share photos!');
      if (hasLinksOrPhone(text)) return socket.emit('sys_msg', '⚠️ Links & phone numbers blocked in anonymous mode.');
    }
    const msg = { senderId: socket.id, senderName: room.isFriends ? socket.userData.name : 'Stranger', text, media: media, time: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }) };
    io.to(socket.roomId).emit('receive_message', msg);
  });

  socket.on('add_friend', () => {
    const room = activeRooms[socket.roomId];
    if (!room || socket.userData.isGuest) return;
    room.votes.add(socket.id);
    socket.to(socket.roomId).emit('sys_msg', '💚 Stranger sent a Friend Request! Click "Add Frnd" to accept.');
    if (room.votes.size === 2) {
      room.isFriends = true;
      const [id1, id2] = room.users;
      const s1 = io.sockets.sockets.get(id1), s2 = io.sockets.sockets.get(id2);
      if (s1 && s2) {
        s1.emit('friend_revealed', s2.userData);
        s2.emit('friend_revealed', s1.userData);
      }
    }
  });

  socket.on('owner_auth', key => {
    if (key === OWNER_KEY) socket.emit('owner_dashboard', { online: io.engine.clientsCount, rooms: Object.keys(activeRooms).length, reports: reportedCases });
  });

  socket.on('disconnect', () => {
    waitingQueue = waitingQueue.filter(s => s.id !== socket.id);
    if (socket.roomId && activeRooms[socket.roomId]) {
      socket.to(socket.roomId).emit('peer_skipped');
      delete activeRooms[socket.roomId];
    }
    io.emit('online_count', io.engine.clientsCount);
  });
});

const PORT = process.env.PORT || 4000;
server.listen(PORT, () => console.log('🚀 ADIPOLI Server Started on port ' + PORT));