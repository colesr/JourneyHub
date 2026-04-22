# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**JourneyHub** — an AI-guided growth network for builders and founders. A single-page app with a vanilla-JS frontend (`index.html`, ~12,000 lines) and a Cloudflare Workers backend (`worker/`). No build step, no framework.

Live at: **https://journeyhub.cole-colesr-sam.workers.dev**

Runs entirely on Cloudflare's permanent free tier:
- **Workers** — API + static asset serving (100k req/day, no sleep, no pause)
- **D1** — SQLite database (5GB, forever free)
- **R2** — image storage (10GB, **zero egress fees**)
- **Workers AI** — Llama 3.1 8B Instruct via `env.AI.run()` on the Worker; users never see or provide an API key

## Architecture

- **`index.html`** — Entire frontend: styles (CSS custom properties in `:root`), HTML shell, and all application JS in a single `<script type="module">` block. Imports from `./shims/firebase-*.js` (not the Firebase CDN) — the shims expose the Firebase v9 modular API but route calls to the Cloudflare Worker. ~140+ handlers are exposed globally via `window.*` at the bottom of the file (~line 11230+).
- **`worker/`** — Cloudflare Worker (Hono + D1 + R2 + Durable Objects).
  - `worker/src/index.js` — routes: `/api/auth/*` (register, login, logout, me), `POST /api/db` (document store), `POST /api/upload` + `GET /r2/*` (image CRUD), `GET /api/ws/:convId` (WebSocket upgrade into a ConversationRoom DO), `POST /api/ai` (Workers AI inference), static asset fallback via the `ASSETS` binding.
  - `worker/src/documents.js` — generic JSON document store backed by a single `documents` table. Firestore-like ops: `getDoc`, `getDocs`, `setDoc`, `updateDoc`, `addDoc`, `deleteDoc`. Constraint evaluation (where/orderBy/limit) runs in-memory on the Worker. After any write to `conversations/<id>/messages`, the matching ConversationRoom DO is notified to broadcast to connected WebSockets.
  - `worker/src/conversation_room.js` — ConversationRoom Durable Object, one per conversation id. Uses the Hibernation API so idle sockets cost zero CPU.
  - `worker/schema.sql` — D1 schema: `users`, `sessions`, `documents` tables.
  - `worker/wrangler.toml` — bindings: D1 (`DB`), R2 (`IMAGES`), Durable Objects (`CONVROOMS`), Workers AI (`AI`), static assets (`ASSETS`).
- **`shims/`** — Frontend shims preserving the Firebase v9 modular API surface so `index.html` never has to change.
  - `firebase-auth.js` — calls `/api/auth/*`, session via `httpOnly` cookie.
  - `firebase-firestore.js` — calls `/api/db`; sentinels (`serverTimestamp`, `arrayUnion`, `increment`, etc.) resolved client-side via read-modify-write. `onSnapshot` on a conversation-messages collection also opens a WebSocket to `/api/ws/:convId` and refetches on each broadcast.
  - `firebase-storage.js` — uploads via `POST /api/upload`, URLs are relative `/r2/<path>`.
  - `firebase-app.js`, `firebase-functions.js` — minimal noop/pass-through shims.
  - `cloud-functions.js` — browser-side implementations of the AI Callable Functions (summarize, improve, mood, community DNA, mentorship, resource search, etc.). Each calls `callAI(prompt, opts)` which POSTs to `/api/ai` on the Worker.
  - `ai.js` — thin client for the Worker's `/api/ai` endpoint. Exports `callAI(prompt, opts)` and `parseJsonLoose(raw)`.
- **`.assetsignore`** — lists files at the repo root the Worker should NOT serve as static assets (e.g. `worker/`, `CLAUDE.md`, `.claude/`).

## Database Model

**Auth tables (typed):**
- `users(id, email, username, password_hash, password_salt, display_name, bio, avatar_url, created_at, updated_at)`
- `sessions(token, user_id, expires_at, created_at)` — 30-day TTL

**Document store (generic):**
- `documents(path, collection, doc_id, data JSON, created_at, updated_at)` — one row per Firestore-style path (e.g. `threads/abc` or `conversations/xyz/messages/m1`). Indexed by `collection` and `(collection, updated_at DESC)`.

Everything the app historically wrote to Firestore (`threads`, `comments`, `communities`, `conversations`/`messages`, `events`, `notifications`, `growthPaths`, `journeyResponses`, `feedPosts`, `investments`, etc.) now lives in `documents`. Type-safety is enforced by the frontend, not the schema.

## Auth

- PBKDF2 (Web Crypto), SHA-256, 100k iterations, per-user 16-byte salt.
- Session: 32-byte random URL-safe token in D1 `sessions` table, mirrored in an `httpOnly` + `Secure` + `SameSite=Lax` cookie (`session`), 30-day TTL.
- `/api/auth/register` creates a user + session in one request; `/api/auth/login` creates a new session; `/api/auth/logout` deletes the current session.
- Hono middleware on `*` hydrates `c.get('user')` from the cookie on every request.

## Real-time

- **Same-client** — writes through the Firestore shim fire client-side listeners immediately via an in-memory event bus.
- **Cross-client (DMs)** — implemented via Durable Objects + hibernating WebSockets. `onSnapshot` on `conversations/<id>/messages` opens a WS that refetches on server push; writes to that collection notify the DO which broadcasts to participants. Other realtime (comments, notifications) still polls on navigation.

## Common Commands

```bash
# Deploy the Worker (hosts API + static assets)
cd worker && npx wrangler deploy

# Apply a schema change to D1
cd worker && npx wrangler d1 execute journeyhub --remote --file=./schema.sql

# Inspect D1 (ad-hoc query)
cd worker && npx wrangler d1 execute journeyhub --remote --command="SELECT COUNT(*) FROM documents;"

# Local dev (local SQLite simulation)
cd worker && npx wrangler dev

# Tail production logs
cd worker && npx wrangler tail

# Re-auth if wrangler token is missing scopes (e.g. r2)
wrangler login
```

## Key Patterns

- **No routing library** — navigation is handled by `show*()` functions that replace `#app` innerHTML.
- **Shim swap, not app rewrite** — the backend changed from Firebase → Cloudflare, but `index.html` is untouched because the shims preserve the Firebase v9 API. Any future backend change should follow the same pattern.
- **All handlers on `window`** — event handlers referenced in inline `onclick=` attributes are assigned at the bottom of the script block.
- **Image URLs are relative** (`/r2/<path>`) so they work across any origin the app is hosted on.
- **Auth state is async on load** — `onAuthStateChanged` callbacks may fire with `null` once before the `/api/auth/me` hydration completes, then again with the real user. App code should handle both.
- **Adding a new Worker endpoint** — route in `worker/src/index.js`; read session via `c.get('user')`; deploy with `wrangler deploy`; update any shim if the frontend needs a new client method.
