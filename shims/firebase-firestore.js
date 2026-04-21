// firebase-firestore.js — Cloudflare Worker / D1 backed shim of the v9 modular
// Firestore API. Public surface is identical to the prior localStorage version;
// storage primitives now call POST /api/db. Sentinels (serverTimestamp,
// arrayUnion, arrayRemove, increment, deleteField) are resolved client-side via
// a read-modify-write cycle.

const DB_URL = '/api/db';

async function dbCall(payload) {
  const res = await fetch(DB_URL, {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.error || `HTTP ${res.status}`);
    err.code = 'firestore/' + res.status;
    throw err;
  }
  return data;
}

// ---------- Timestamp ----------
export class Timestamp {
  constructor(seconds, nanoseconds = 0) {
    this.seconds = seconds;
    this.nanoseconds = nanoseconds;
    this._seconds = seconds;
  }
  toMillis() { return this.seconds * 1000 + Math.floor(this.nanoseconds / 1e6); }
  toDate() { return new Date(this.toMillis()); }
  valueOf() { return this.toMillis(); }
  static now() { return Timestamp.fromMillis(Date.now()); }
  static fromMillis(m) { return new Timestamp(Math.floor(m / 1000), (m % 1000) * 1e6); }
  static fromDate(d) { return Timestamp.fromMillis(d.getTime()); }
  toJSON() { return { __t: 'ts', m: this.toMillis() }; }
}

function hydrateValue(v) {
  if (v === null || v === undefined) return v;
  if (Array.isArray(v)) return v.map(hydrateValue);
  if (typeof v === 'object') {
    if (v.__t === 'ts' && typeof v.m === 'number') return Timestamp.fromMillis(v.m);
    const out = {};
    for (const k of Object.keys(v)) out[k] = hydrateValue(v[k]);
    return out;
  }
  return v;
}

function dehydrateValue(v) {
  if (v === null || v === undefined) return v;
  if (v instanceof Timestamp) return { __t: 'ts', m: v.toMillis() };
  if (v instanceof Date) return { __t: 'ts', m: v.getTime() };
  if (Array.isArray(v)) return v.map(dehydrateValue);
  if (typeof v === 'object') {
    const out = {};
    for (const k of Object.keys(v)) out[k] = dehydrateValue(v[k]);
    return out;
  }
  return v;
}

// ---------- Sentinels (FieldValue equivalents) ----------
const SENTINEL = Symbol.for('fbShimSentinel');
export function serverTimestamp() { return { [SENTINEL]: 'serverTimestamp' }; }
export function arrayUnion(...values) { return { [SENTINEL]: 'arrayUnion', values }; }
export function arrayRemove(...values) { return { [SENTINEL]: 'arrayRemove', values }; }
export function deleteField() { return { [SENTINEL]: 'deleteField' }; }
export function increment(n) { return { [SENTINEL]: 'increment', value: n }; }
function isSentinel(v) { return v && typeof v === 'object' && v[SENTINEL]; }

function deepEqual(a, b) {
  if (a === b) return true;
  if (a == null || b == null) return false;
  if (typeof a !== typeof b) return false;
  if (typeof a !== 'object') return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  if (Array.isArray(a)) {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) if (!deepEqual(a[i], b[i])) return false;
    return true;
  }
  const ka = Object.keys(a);
  const kb = Object.keys(b);
  if (ka.length !== kb.length) return false;
  for (const k of ka) if (!deepEqual(a[k], b[k])) return false;
  return true;
}

function applyWrites(existing, patch, mode) {
  const result = mode === 'merge' || mode === 'update' ? { ...(existing || {}) } : {};
  for (const key of Object.keys(patch)) {
    const v = patch[key];
    if (isSentinel(v)) {
      const op = v[SENTINEL];
      if (op === 'serverTimestamp') {
        result[key] = Timestamp.now();
      } else if (op === 'deleteField') {
        delete result[key];
      } else if (op === 'arrayUnion') {
        const arr = Array.isArray(result[key]) ? [...result[key]] : [];
        for (const x of v.values) {
          if (!arr.some((item) => deepEqual(item, x))) arr.push(x);
        }
        result[key] = arr;
      } else if (op === 'arrayRemove') {
        const arr = Array.isArray(result[key]) ? [...result[key]] : [];
        result[key] = arr.filter((item) => !v.values.some((x) => deepEqual(item, x)));
      } else if (op === 'increment') {
        const cur = typeof result[key] === 'number' ? result[key] : 0;
        result[key] = cur + v.value;
      }
    } else if (mode === 'merge' && v && typeof v === 'object' && !Array.isArray(v) && !(v instanceof Timestamp) && !(v instanceof Date)) {
      const cur = result[key] && typeof result[key] === 'object' && !Array.isArray(result[key]) ? result[key] : {};
      result[key] = applyWrites(cur, v, 'merge');
    } else {
      result[key] = v;
    }
  }
  return result;
}

// Detect whether a patch contains any sentinels (requires a read-modify-write cycle)
function patchNeedsExisting(patch, mode) {
  if (mode === 'merge' || mode === 'update') return true;
  for (const key of Object.keys(patch)) {
    if (isSentinel(patch[key])) return true;
  }
  return false;
}

const AUTO_ID_CHARS = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
function autoId() {
  const buf = new Uint8Array(20);
  crypto.getRandomValues(buf);
  let s = '';
  for (let i = 0; i < 20; i++) s += AUTO_ID_CHARS[buf[i] % AUTO_ID_CHARS.length];
  return s;
}

// ---------- Refs ----------
class CollectionReference {
  constructor(path) {
    this.path = path;
    this.id = path.split('/').pop();
    this.type = 'collection';
    this._isCollection = true;
  }
}
class DocumentReference {
  constructor(path) {
    this.path = path;
    const segs = path.split('/');
    this.id = segs[segs.length - 1];
    this.parent = new CollectionReference(segs.slice(0, -1).join('/'));
    this.type = 'document';
    this._isDoc = true;
  }
}

export function getFirestore(_app) { return { __isDb: true }; }

export function collection(dbOrRef, ...segments) {
  let basePath = '';
  if (dbOrRef && dbOrRef._isDoc) basePath = dbOrRef.path + '/';
  const flat = segments.join('/').replace(/\/+/g, '/').replace(/^\/|\/$/g, '');
  return new CollectionReference(basePath + flat);
}

export function doc(dbOrRef, ...segments) {
  if (dbOrRef && dbOrRef._isCollection) {
    const id = segments[0] || autoId();
    return new DocumentReference(dbOrRef.path + '/' + id);
  }
  if (dbOrRef && dbOrRef._isDoc) {
    return new DocumentReference(dbOrRef.path + '/' + segments.join('/'));
  }
  const flat = segments.join('/').replace(/\/+/g, '/').replace(/^\/|\/$/g, '');
  return new DocumentReference(flat);
}

// ---------- Snapshots ----------
class DocumentSnapshot {
  constructor(ref, raw) {
    this.ref = ref;
    this.id = ref.id;
    this._raw = raw;
  }
  exists() { return this._raw != null; }
  data() { return this._raw == null ? undefined : hydrateValue(this._raw); }
  get(field) {
    const d = this.data();
    if (!d) return undefined;
    return field.split('.').reduce((acc, k) => (acc == null ? undefined : acc[k]), d);
  }
}

class QuerySnapshot {
  constructor(docs) {
    this.docs = docs;
    this.empty = docs.length === 0;
    this.size = docs.length;
  }
  forEach(cb) { this.docs.forEach(cb); }
}

// ---------- Query builder ----------
export function query(collectionRef, ...constraints) {
  return { __type: 'query', _collection: collectionRef, _constraints: constraints };
}
export function where(field, op, value) { return { kind: 'where', field, op, value: dehydrateValue(value) }; }
export function orderBy(field, direction = 'asc') { return { kind: 'orderBy', field, direction }; }
export function limit(count) { return { kind: 'limit', count }; }

// ---------- Reads ----------
export async function getDoc(ref) {
  const { data } = await dbCall({ op: 'getDoc', path: ref.path });
  return new DocumentSnapshot(ref, data);
}

export async function getDocs(refOrQuery) {
  let coll;
  let constraints = [];
  if (refOrQuery && refOrQuery._isCollection) {
    coll = refOrQuery;
  } else if (refOrQuery && refOrQuery.__type === 'query') {
    coll = refOrQuery._collection;
    constraints = refOrQuery._constraints;
  } else {
    throw new Error('getDocs: expected CollectionReference or Query');
  }
  const { docs } = await dbCall({
    op: 'getDocs',
    collection: coll.path,
    constraints,
  });
  const snaps = docs.map((d) => new DocumentSnapshot(new DocumentReference(coll.path + '/' + d.id), d.data));
  return new QuerySnapshot(snaps);
}

// ---------- Writes ----------
export async function setDoc(ref, data, options = {}) {
  const merge = !!options.merge;
  let payload;
  if (patchNeedsExisting(data, merge ? 'merge' : 'set')) {
    const { data: existing } = await dbCall({ op: 'getDoc', path: ref.path });
    const existingHydrated = existing ? hydrateValue(existing) : null;
    const wasNew = existing == null;
    payload = applyWrites(merge ? existingHydrated : null, data, merge ? 'merge' : 'set');
    await dbCall({ op: 'setDoc', path: ref.path, data: dehydrateValue(payload), merge: false });
    emit(ref.path.split('/').slice(0, -1).join('/'), wasNew ? 'create' : 'update', ref, payload);
  } else {
    // No sentinels and no merge → plain set
    await dbCall({ op: 'setDoc', path: ref.path, data: dehydrateValue(data), merge: false });
    emit(ref.path.split('/').slice(0, -1).join('/'), 'create', ref, data);
  }
}

export async function updateDoc(ref, data) {
  const { data: existing } = await dbCall({ op: 'getDoc', path: ref.path });
  if (existing == null) throw new Error('updateDoc: document does not exist at ' + ref.path);
  const next = applyWrites(hydrateValue(existing), data, 'update');
  await dbCall({ op: 'setDoc', path: ref.path, data: dehydrateValue(next), merge: false });
  emit(ref.path.split('/').slice(0, -1).join('/'), 'update', ref, next);
}

export async function addDoc(collRef, data) {
  let payload = data;
  if (patchNeedsExisting(data, 'set')) {
    payload = applyWrites(null, data, 'set');
  }
  const { id, path } = await dbCall({
    op: 'addDoc',
    collection: collRef.path,
    data: dehydrateValue(payload),
  });
  const ref = new DocumentReference(path);
  emit(collRef.path, 'create', ref, payload);
  return ref;
}

export async function deleteDoc(ref) {
  await dbCall({ op: 'deleteDoc', path: ref.path });
  emit(ref.path.split('/').slice(0, -1).join('/'), 'delete', ref, null);
}

// ---------- onSnapshot via client-side event bus ----------
// Same-client real-time: writes via this shim notify locally-attached listeners.
// Cross-client real-time arrives with DMs in Phase 4 (Durable Objects).
const collectionListeners = new Map();
const docListeners = new Map();
const docTriggers = [];

function emit(collPath, kind, ref, data) {
  const setC = collectionListeners.get(collPath);
  if (setC) for (const fn of setC) { try { fn(); } catch (e) { console.error(e); } }
  const setD = docListeners.get(ref.path);
  if (setD) for (const fn of setD) { try { fn(); } catch (e) { console.error(e); } }
  if (kind === 'create') {
    for (const trig of docTriggers) {
      const m = ref.path.match(trig.regex);
      if (m) {
        const params = {};
        trig.paramNames.forEach((name, i) => { params[name] = m[i + 1]; });
        Promise.resolve().then(() => trig.fn({ params, data, ref })).catch((e) => console.error(e));
      }
    }
  }
}

const MESSAGES_COLL_RE = /^conversations\/([^/]+)\/messages$/;

export function onSnapshot(refOrQuery, cb, errCb) {
  let coll;
  let isDoc = false;
  let docPath = null;
  if (refOrQuery && refOrQuery._isDoc) {
    isDoc = true;
    docPath = refOrQuery.path;
  } else if (refOrQuery && refOrQuery._isCollection) {
    coll = refOrQuery;
  } else if (refOrQuery && refOrQuery.__type === 'query') {
    coll = refOrQuery._collection;
  } else {
    throw new Error('onSnapshot: invalid argument');
  }

  const fire = async () => {
    try {
      if (isDoc) cb(await getDoc(refOrQuery));
      else cb(await getDocs(refOrQuery));
    } catch (e) {
      if (errCb) errCb(e); else console.error('onSnapshot fire error', e);
    }
  };

  if (isDoc) {
    if (!docListeners.has(docPath)) docListeners.set(docPath, new Set());
    docListeners.get(docPath).add(fire);
  } else {
    if (!collectionListeners.has(coll.path)) collectionListeners.set(coll.path, new Set());
    collectionListeners.get(coll.path).add(fire);
  }

  // Cross-client realtime: open a WS when listening on a messages collection.
  // On any notification, refetch so cb sees the full, ordered current state.
  let ws = null;
  let wsPingTimer = null;
  if (!isDoc && coll) {
    const m = coll.path.match(MESSAGES_COLL_RE);
    if (m) {
      try {
        const convId = m[1];
        const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
        ws = new WebSocket(`${scheme}//${location.host}/api/ws/${convId}`);
        ws.addEventListener('message', () => fire());
        ws.addEventListener('open', () => {
          wsPingTimer = setInterval(() => {
            try { ws.send('ping'); } catch {}
          }, 30_000);
        });
        ws.addEventListener('close', () => {
          if (wsPingTimer) { clearInterval(wsPingTimer); wsPingTimer = null; }
        });
        ws.addEventListener('error', (e) => console.warn('messages ws error', e));
      } catch (e) {
        console.warn('messages ws setup failed', e);
      }
    }
  }

  fire();

  return () => {
    if (isDoc) docListeners.get(docPath)?.delete(fire);
    else collectionListeners.get(coll.path)?.delete(fire);
    if (wsPingTimer) { clearInterval(wsPingTimer); wsPingTimer = null; }
    if (ws) {
      try { ws.close(); } catch {}
      ws = null;
    }
  };
}

// Document trigger registry (used by cloud-functions shim)
export function registerDocumentTrigger(pattern, fn) {
  const paramNames = [];
  const regexStr = '^' + pattern.replace(/\{([^}]+)\}/g, (_, name) => {
    paramNames.push(name);
    return '([^/]+)';
  }) + '$';
  docTriggers.push({ pattern, regex: new RegExp(regexStr), paramNames, fn });
}

export const db = { __isDb: true };
