# Frontistirio — Real-Time Sync (Socket.IO + Change Streams) Brain

> **Purpose:** Documents how the real-time data synchronization system works — from MongoDB Change Streams through Socket.IO to the frontend. Reference this when debugging live updates, adding new real-time events, or understanding the watcher architecture.

---

## Overview

The backend maintains **live data sync** between the database and all connected clients using:
- **MongoDB Change Streams** — watch for DB changes at the collection level
- **Socket.IO** — push changes to specific connected users
- **User socket registry** — map of `userId → socketId` to target specific users

```
MongoDB Change → watchXxx() handler → emitToSpecificUser() → Socket.IO → Frontend
```

---

## Architecture

### Server Setup (`index.js`)

```javascript
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: [...] } });

// In-memory user socket map
const userSockets = {};

io.on("connection", (socket) => {
  socket.on("register", ({ userId, role, storeId }) => {
    userSockets[userId] = { socketId: socket.id, role, storeId };
  });

  socket.on("disconnect", () => {
    // Remove user from userSockets map
  });
});

app.set("io", io); // make io accessible in controllers
```

### User Registration Flow

1. User logs in → receives JWT
2. Frontend connects to Socket.IO
3. Frontend emits `register` event with `{ userId, role, storeId }`
4. Server stores mapping: `userSockets[userId] = { socketId, role, storeId }`
5. Now the server can push targeted events to this user

---

## Watcher Functions

Each major collection has a `watchXxx(io, userSockets)` function started at server boot:

| Function | Collection | Started In |
|---|---|---|
| `watchStudents` | `Student` | `index.js` line ~160 |
| `watchTeachers` | `Teacher` | `index.js` |
| `watchGroups` | `Group` | `index.js` |
| `watchStores` | `Store` | `index.js` |
| `watchGrades` | `Grade` | `index.js` |
| `watchClasses` | `ClassModel` | `index.js` |
| `watchCourses` | `Course` | `index.js` |
| `watchTeachingPeriods` | `TeachingPeriod` | `index.js` |
| `watchEducationalMaterial` | `EducationalMaterial` | `index.js` |
| `watchStoreSettings` | `StoreSettings` | `index.js` |
| `watchClassRooms` | `Room` | `index.js` |
| `watchTestCycles` | `TestCycle` | `index.js` |

---

## Watcher Pattern

Each watcher follows the same structure:

```javascript
exports.watchStudents = async (io, userSockets) => {
  const changeStream = Student.watch([], { fullDocument: "updateLookup" });

  changeStream.on("change", async (change) => {
    const operationType = change.operationType; // "insert", "update", "delete"
    const document = change.fullDocument;

    // Determine which store_id this belongs to
    const store_id = document?.store_id?.toString();

    // Find all connected users for this store
    for (const [userId, info] of Object.entries(userSockets)) {
      if (info.storeId?.toString() === store_id) {
        emitToSpecificUser(io, userId, "studentsUpdated", { change }, userSockets);
      }
    }
  });
};
```

Key details:
- `{ fullDocument: "updateLookup" }` — ensures the full updated document is included in the change event
- The watcher identifies which store the changed document belongs to, then notifies only users of that store
- No broadcasting to all clients — always **store-scoped** delivery

---

## `emitToSpecificUser` Helper (`helpers/emitToSpecificUser.js`)

```javascript
function emitToSpecificUser(io, userId, event, data, userSockets) {
  const socketId = userSockets[userId].socketId;
  if (socketId) {
    io.to(socketId).emit(event, data);
  }
}
```

Targets a specific socket by `socketId`. If the user is not connected, the event is silently dropped.

---

## Socket.IO Events Reference

| Event Name (server → client) | Trigger |
|---|---|
| `studentsUpdated` | Any Student document change |
| `teachersUpdated` | Any Teacher document change |
| `groupsUpdated` | Any Group document change |
| `storesUpdated` | Any Store document change |
| `gradesUpdated` | Any Grade document change |
| `classesUpdated` | Any ClassModel document change |
| `coursesUpdated` | Any Course document change |
| `teachingPeriodsUpdated` | Any TeachingPeriod document change |
| `educationalMaterialUpdated` | Any EducationalMaterial document change |
| `storeSettingsUpdated` | Any StoreSettings document change |
| `classRoomsUpdated` | Any Room document change |
| `testCyclesUpdated` | Any TestCycle document change |

| Event Name (client → server) | Purpose |
|---|---|
| `register` | Register user's socket after login |
| `disconnect` | Auto-fired by Socket.IO on disconnect |

---

## CORS Configuration for Socket.IO

```javascript
const io = new Server(server, {
  cors: {
    origin: [
      "http://localhost:8100",   // Ionic dev
      "http://localhost:4200",   // Angular dev
      "https://d3tf4mtxs2b8zy.cloudfront.net"  // Production (CloudFront)
    ],
    methods: ["GET", "POST"]
  }
});
```

---

## Frontend Integration Points

The frontend should:
1. Connect to Socket.IO on app load
2. Emit `register` immediately after login with `{ userId, role, storeId }`
3. Listen for the relevant update events and refresh local state/store accordingly
4. Handle disconnect/reconnect (re-emit `register` on reconnect)

---

## Known Limitations

- `userSockets` is **in-memory** — lost on server restart
- No persistence or retry mechanism for missed events
- If a user is not connected when an event fires, the update is lost (they'll get fresh data on next page load via REST)
- Horizontal scaling would require a Socket.IO adapter (e.g., Redis) — currently single-instance only
