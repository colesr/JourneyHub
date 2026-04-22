// firebase-auth.js — Cloudflare Worker backed shim.
// The Worker owns the session (httpOnly cookie). This module just makes fetch
// calls with credentials: 'include' and caches the current user in memory.

function toFirebaseUser(apiUser) {
  if (!apiUser) return null;
  return {
    uid: apiUser.id,
    email: apiUser.email,
    displayName: apiUser.username || null,
    photoURL: apiUser.avatarUrl || null,
    emailVerified: true,
    providerData: [{ providerId: 'password' }],
  };
}

function mapError(msg, status) {
  if (!msg) return 'auth/unknown';
  if (/already taken/i.test(msg)) return 'auth/email-already-in-use';
  if (/invalid credentials/i.test(msg)) return 'auth/wrong-password';
  if (/at least 8 characters/i.test(msg)) return 'auth/weak-password';
  if (/missing/i.test(msg)) return 'auth/invalid-email';
  return `auth/http-${status}`;
}

async function apiFetch(path, opts = {}) {
  const res = await fetch(path, {
    method: opts.method || 'GET',
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...(opts.headers || {}) },
    body: opts.body,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.error || `HTTP ${res.status}`);
    err.code = mapError(data.error, res.status);
    throw err;
  }
  return data;
}

class AuthInstance {
  constructor() {
    this.currentUser = null;
    this._listeners = new Set();
    this._hydrated = false;
    this._hydratePromise = this._hydrate();
  }
  async _hydrate() {
    try {
      const { user } = await apiFetch('/api/auth/me');
      this.currentUser = toFirebaseUser(user);
    } catch {
      this.currentUser = null;
    }
    this._hydrated = true;
    this._notify();
  }
  _notify() {
    for (const cb of this._listeners) {
      try { cb(this.currentUser); } catch (e) { console.error(e); }
    }
  }
}

let _instance = null;
export function getAuth() {
  if (!_instance) _instance = new AuthInstance();
  return _instance;
}

export function onAuthStateChanged(auth, cb) {
  auth._listeners.add(cb);
  if (auth._hydrated) {
    Promise.resolve().then(() => cb(auth.currentUser));
  }
  return () => auth._listeners.delete(cb);
}

function deriveUsername(email) {
  const prefix = (email.split('@')[0] || 'user').toLowerCase().replace(/[^a-z0-9_]/g, '');
  if (prefix.length >= 3) return prefix.slice(0, 20);
  return 'user' + Math.floor(Math.random() * 100000);
}

export async function createUserWithEmailAndPassword(auth, email, password) {
  const normEmail = (email || '').trim().toLowerCase();
  const data = await apiFetch('/api/auth/register', {
    method: 'POST',
    body: JSON.stringify({ email: normEmail, password, username: deriveUsername(normEmail) }),
  });
  auth.currentUser = toFirebaseUser(data);
  auth._notify();
  return { user: auth.currentUser };
}

export async function signInWithEmailAndPassword(auth, email, password) {
  const data = await apiFetch('/api/auth/login', {
    method: 'POST',
    body: JSON.stringify({ email: (email || '').trim().toLowerCase(), password }),
  });
  auth.currentUser = toFirebaseUser(data);
  auth._notify();
  return { user: auth.currentUser };
}

export async function signOut(auth) {
  await apiFetch('/api/auth/logout', { method: 'POST' });
  auth.currentUser = null;
  auth._notify();
}

export async function sendPasswordResetEmail(_auth, _email) {
  const err = new Error('Password reset not yet wired up in Cloudflare backend.');
  err.code = 'auth/operation-not-supported';
  throw err;
}

export async function sendEmailVerification(_user) {
  // Auto-verified in this build.
}

export async function deleteUser(_user) {
  const err = new Error('Account deletion not yet wired up in Cloudflare backend.');
  err.code = 'auth/operation-not-supported';
  throw err;
}

