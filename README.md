# Frontistirio — Real-Time Sync Brain

> **Full-stack reference** for the entire real-time pipeline: MongoDB Change Streams → backend watcher functions → Socket.IO → Angular NgRx store. Covers every event, the exact watcher logic, reconnection strategy, and the connection-lost UI.

---

## Architecture

```
MongoDB Change Stream (insert / update / delete)
       ↓
watchXxx(io, userSockets) — runs on server boot, one per collection
       ↓
Filter: only users of the affected store_id receive the event
       ↓
emitToSpecificUser(io, userId, eventName, data, userSockets)
       ↓
socket.io-client receives event in WebSocketService
       ↓
store.dispatch(NgRxAction)
       ↓
Reducer updates AppState slice
       ↓
Component selectors → reactive re-render
```

---

## Backend: Socket.IO Server (`index.js`)

```javascript
const io = new Server(server, {
  cors: {
    origin: [
      'http://localhost:8100',    // Ionic dev
      'http://localhost:4200',    // Angular dev
      'https://d3tf4mtxs2b8zy.cloudfront.net'  // CloudFront prod
    ],
    methods: ['GET', 'POST']
  }
});

// In-memory registry: userId → { socketId, role, storeId }
const userSockets = {};

io.on('connection', (socket) => {
  // Client emits this immediately after login
  socket.on('register', ({ userId, role, storeId }) => {
    if (!userId) return; // guard: never register without userId
    userSockets[userId] = { socketId: socket.id, role, storeId };
  });

  socket.on('disconnect', () => {
    for (const [userId, info] of Object.entries(userSockets)) {
      if (info.socketId === socket.id) delete userSockets[userId];
    }
  });
});

app.set('io', io); // accessible in controllers via req.app.get('io')
```

All watchers are started at server boot, after MongoDB connects:
```javascript
watchGroups(io, userSockets);
watchStores(io, userSockets);
watchGrades(io, userSockets);
watchClasses(io, userSockets);
watchCourses(io, userSockets);
watchTeachingPeriods(io, userSockets);
watchEducationalMaterial(io, userSockets);
watchStudents(io, userSockets);
watchTeachers(io, userSockets);
watchStoreSettings(io, userSockets);
watchClassRooms(io, userSockets);
watchTestCycles(io, userSockets);
```

---

## Backend: Watcher Pattern (detailed — from `watchStudents`)

Each watcher uses `{ fullDocument: 'updateLookup', fullDocumentBeforeChange: 'required' }`:

```javascript
exports.watchStudents = async (io, userSockets) => {
  const changeStream = Student.watch([], {
    fullDocument: 'updateLookup',          // get full doc on updates
    fullDocumentBeforeChange: 'required'   // get doc before delete
  });

  changeStream.on('change', async (change) => {

    // INSERT: populate and emit full document
    if (change.operationType === 'insert') {
      const newStudent = change.fullDocument;
      const populatedStudent = await Student.findById(newStudent._id)
        .populate('user_id').populate('parents').lean();

      const storeUsers = Object.keys(userSockets).filter(userId =>
        isStoreUser(userId) &&
        getUserStoreId(userId, userSockets) === newStudent.store_id.toString()
      );
      storeUsers.forEach(userId =>
        emitToSpecificUser(io, userId, 'studentAdded', populatedStudent, userSockets)
      );
    }

    // UPDATE: emit only the changed fields (delta)
    if (change.operationType === 'update') {
      const updatedStudentId = change.documentKey._id.toString();
      const updatedFields = change.updateDescription.updatedFields;
      const store_id = updatedFields.store_id || await getStoreIdForStudent(updatedStudentId);

      // emit { id, changes } — frontend merges into existing NgRx record
      storeUsers.forEach(userId =>
        emitToSpecificUser(io, userId, 'studentUpdated',
          { id: updatedStudentId, changes: updatedFields }, userSockets)
      );
    }

    // DELETE: use fullDocumentBeforeChange to know the store_id
    if (change.operationType === 'delete') {
      const deletedStudent = change.fullDocumentBeforeChange;
      // emit id → frontend removes from NgRx array
      emitToSpecificUser(io, userId, 'studentDeleted', deletedStudent._id, userSockets);
    }
  });
};
```

**Key design decisions:**
- `fullDocument: 'updateLookup'` — re-fetches the full doc from DB; without it, updates only contain the diff
- `fullDocumentBeforeChange` — needed for delete so we know which `store_id` to target
- On `insert`: emits the **full populated document** (parents populated)
- On `update`: emits only `{ id, updatedFields }` — a **delta** — so the frontend merges it
- On `delete`: emits the MongoDB `_id` string

### `emitToSpecificUser` helper (`helpers/emitToSpecificUser.js`)
```javascript
function emitToSpecificUser(io, userId, event, data, userSockets) {
  const socketId = userSockets[userId]?.socketId;
  if (socketId) io.to(socketId).emit(event, data);
  // silently drops if user is not connected
}
```

### `isStoreUser` / `getUserStoreId` helpers
Used to filter `userSockets` to only the relevant store's connected users:
```javascript
// helpers/isStoreUser.js
function isStoreUser(userId) {
  return userSockets[userId]?.role === 'store-user';
}
// helpers/getUserStoreId.js
function getUserStoreId(userId, userSockets) {
  return userSockets[userId]?.storeId?.toString();
}
```

---

## Complete Event Reference

| Backend emits | Trigger | Frontend receives → NgRx action |
|---|---|---|
| `groupAdded` | Group insert | `GroupActions.addGroup({ group })` |
| `groupUpdated` | Group update | `GroupActions.updateGroupFields({ id, changes })` |
| `groupDeleted` | Group delete | `GroupActions.deleteGroup(id)` |
| `storeAdded` | Store insert | `StoresActions.addStore({ store })` |
| `storeUpdated` | Store update | `StoresActions.updateStoreFields({ id, changes })` |
| `storeDeleted` | Store delete | `StoresActions.deleteStore(id)` |
| `gradeAdded` | Grade insert | `GradesActions.addGrade({ grade })` |
| `gradeUpdated` | Grade update | `GradesActions.updateGradeFields({ id, changes })` |
| `gradeDeleted` | Grade delete | `GradesActions.deleteGrade(id)` |
| `classAdded` | ClassModel insert | `ClassActions.addClass({ newClass })` |
| `classUpdated` | ClassModel update | `ClassActions.updateClassFields({ id, changes })` |
| `classDeleted` | ClassModel delete | `ClassActions.deleteClass(id)` |
| `courseAdded` | Course insert | `CourseActions.addCourse({ course })` |
| `courseUpdated` | Course update | `CourseActions.updateCourseFields({ id, changes })` |
| `courseDeleted` | Course delete | `CourseActions.deleteCourse(id)` |
| `studentAdded` | Student insert | `StudentActions.addStudent({ student })` |
| `studentUpdated` | Student update | `StudentActions.updateStudentFields({ id, changes })` |
| `studentDeleted` | Student delete | `StudentActions.deleteStudent(id)` |
| `teacherAdded` | Teacher insert | `TeacherActions.addTeacher({ teacher })` |
| `teacherUpdated` | Teacher update | `TeacherActions.updateTeacherFields({ id, changes })` |
| `teacherDeleted` | Teacher delete | `TeacherActions.deleteTeacher(id)` |
| `periodAdded` | TeachingPeriod insert | `TeachingPeriodsActions.addTeachingPeriod({ teaching_period })` |
| `periodUpdated` | TeachingPeriod update | `TeachingPeriodsActions.updatePeriodFields({ id, changes })` |
| `periodDeleted` | TeachingPeriod delete | `TeachingPeriodsActions.deleteTeachingPeriod(id)` |
| `materialAdded` | EducationalMaterial insert | `EducationalMaterialActions.addEducationalMaterial({ material })` |
| `materialUpdated` | EducationalMaterial update | `EducationalMaterialActions.updateMaterialFields({ id, changes })` |
| `materialDeleted` | EducationalMaterial delete | `EducationalMaterialActions.deleteEducationalMaterial(id)` |
| `testCycleAdded` | TestCycle insert | `TestCyclesActions.addTestCycle({ test_cycle })` |
| `testCycleUpdated` | TestCycle update | `TestCyclesActions.updatePeriodFields({ id, changes })` |
| `testCycleDeleted` | TestCycle delete | `TestCyclesActions.deleteTestCycle(id)` |
| `settingsAdded` | StoreSettings insert | `StoreSettingsActions.setStoreSettings({ settings })` |
| `settingsUpdated` | StoreSettings update | `StoreSettingsActions.editStoreSettings({ id, changes })` |

---

## Frontend: WebSocketService (`services/web-socket.service.ts`)

### Connection
```typescript
public connect(userId: string, role: string, storeId: string | undefined) {
  this.socket = io(environment.wsUrl, {
    transports: ['websocket'],  // no polling fallback
    secure: true,
    reconnection: true,
    reconnectionAttempts: Infinity,  // never gives up
    reconnectionDelay: 1000,         // 1s initial
    reconnectionDelayMax: 5000       // max 5s between attempts
  });

  this.socket.on('connect', () => {
    this.socket.emit('register', { userId, role, storeId });
    this.connectionStatus$.next('connected');
    this.hasConnectedOnce = true;
  });

  // On reconnect: re-register to restore server-side userSockets mapping
  this.socket.io.on('reconnect', () => {
    this.socket.emit('register', this.userId); // ⚠️ Note: only sends userId, not full object
    this.connectionStatus$.next('connected');
  });
}
```

### Connection Status Observable
```typescript
type ConnectionStatus = 'idle' | 'connected' | 'reconnecting' | 'disconnected' | 'error';
private connectionStatus$ = new BehaviorSubject<ConnectionStatus>('idle');
public getConnectionStatus() { return this.connectionStatus$.asObservable(); }
```

Status transitions:
- `'idle'` → initial state, after logout
- `'connected'` → on `connect` event
- `'reconnecting'` → on `reconnect_attempt` (only if `hasConnectedOnce`)
- `'error'` → on `connect_error` or `reconnect_error`
- `'disconnected'` → on `reconnect_failed`

### Disconnect
```typescript
public disconnect() {
  if (this.socket) { this.socket.disconnect(); this.socket = null; }
  this.connectionStatus$.next('idle');
  this.hasConnectedOnce = false;
}
```

---

## Frontend: NgRx Store Shape (`state/app.reducer.ts`)

```typescript
interface AppState {
  students: StudentsState;             // { students: Student[] }
  teachers: TeacherState;              // { teachers: Teacher[] }
  teaching_periods: TeachingPeriodsState; // { teaching_periods: TeachingPeriod[] }
  grades: GradeState;                  // { grades: Grade[] }
  classes: ClassesState;               // { classes: Class[] }
  courses: CoursesState;               // { courses: Course[] }
  groups: GroupState;                  // { groups: Group[] }
  user: UserState;                     // { user: User | null }
  stores: StoresState;                 // { stores: Store[] }
  gradeCategories: GradeCategoryState; // { gradeCategories: GradeCategory[] }
  gradeScenarios: GradeScenarioState;  // { gradeScenarios: GradeScenario[] }
  gradeScales: GradeScalesState;       // { gradeScales: GradeScale[] }
  educational_materials: EducationalMaterialState; // { educational_materials: EducationalMaterial[] }
  test_cycles: TestCycleState;         // { test_cycles: TestCycle[] }
  classrooms: ClassroomsState;         // { classrooms: Classroom[] }
  storeSettings: StoreSettingsState;   // { settings: StoreSettings | null }
}
```

All reducers handle both the bootstrap `setXxx` action (replaces array) and the real-time `addXxx / updateXxxFields / deleteXxx` actions (partial mutations).

Example — `updateStudentFields` reducer:
```typescript
on(StudentsActions.updateStudentFields, (state, { id, changes }) => ({
  ...state,
  students: state.students.map(s => s._id === id ? { ...s, ...changes } : s)
}))
```

---

## Frontend: ConnectionStatus UI Component

`connection-status.component.ts` — embedded in `app.component.html`, visible globally:

```typescript
ngOnInit() {
  this.status$ = this.wsService.getConnectionStatus();
}

ngAfterViewChecked() {
  if (this.lastStatus !== 'connected' && this.lottieContainer && !this.containerRendered) {
    // Load Lottie animation from assets/animations/connection-lost.json
    this.animation = lottie.loadAnimation({
      container: this.lottieContainer.nativeElement,
      renderer: 'svg', loop: true, autoplay: true,
      path: 'assets/animations/connection-lost.json'
    });
    this.containerRendered = true;
  }
  if (this.lastStatus === 'connected' && this.animation) {
    this.animation.stop();
    this.containerRendered = false;
  }
}
```

Shows a full-screen semi-transparent overlay with the animation when not connected. Disappears automatically on reconnection.

---

## Known Gaps & Limitations

- `userSockets` is **in-memory** — a server restart clears all registrations; clients must reconnect and re-register
- On reconnect, the frontend re-emits `register` but only passes `userId` (not the full `{ userId, role, storeId }` object) — this could result in incomplete `userSockets` entry after reconnect
- `ClassRooms` are watched but the backend emits `classRoomsUpdated` (single generic event) rather than the granular `Added/Updated/Deleted` pattern
- No queuing for missed events during disconnect — the client gets stale data until next navigation triggers a REST fetch
- Horizontal scaling is not supported — would require a Socket.IO Redis adapter
