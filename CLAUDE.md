# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**JourneyHub** — an AI-guided growth network for builders and founders. A single-page app built with vanilla JavaScript and Firebase services (no build step, bundler, or framework). The entire frontend lives in `index.html` (~12,000 lines of inline CSS + JS). Gemini-powered AI features run in Firebase Cloud Functions.

## Architecture

- **`index.html`** — The entire frontend: styles (CSS custom properties in `:root`), HTML shell, and all application JS in a single `<script type="module">` block. Uses Firebase JS SDK loaded from CDN (`gstatic.com/firebasejs/10.7.1/`). All view functions render by setting `innerHTML` on `#app`. ~140+ handlers are exposed globally via `window.*` assignments at the bottom of the file (~line 11230+).
- **`functions/index.js`** — Firebase Cloud Functions (Node.js, ~3,000 lines, ~48 exports). Callable functions for Gemini-powered AI features (summarize, improve comment, mood analysis, community DNA, mentorship matching, draft suggestions, etc.) plus Firestore triggers for bot replies, notifications, and badge updates. `GEMINI_API_KEY` is stored as a Firebase secret via `defineSecret`.
- **`firestore.rules`** — Security rules for all Firestore collections (see list below).
- **`storage.rules`** — Firebase Storage rules (image uploads, 5MB limit).
- **`firestore.indexes.json`** — Composite indexes for Firestore queries.
- **`firebase.json`** — Firebase config: hosting serves from `.` with SPA rewrite, plus Firestore and Storage rule references.

### Alternate Railway deployment

The app can also be deployed as a static site to Railway (not just Firebase Hosting):

- **`server.js`** + **`package.json`** (root) — tiny Express static server for Railway.
- **`railway.json`** — Railway build/deploy config (Nixpacks).
- **`shims/`** — drop-in replacements for Firebase/Cloud Function calls when running without Firebase. `gemini.js` calls the Gemini API directly from the browser; `api-key-modal.js` prompts the user for a Gemini key stored in `localStorage`.

## Firestore Collections

Core content:
- `users` — profiles with username, bio, interests, expertise, followers/following, badges
- `threads` — forum posts (title, content, author, communityId, tags, likedBy, imageUrl)
- `comments` — tied to threads via `threadId`, with `likedBy`
- `communities` — user-created groups (name, description, members)
- `conversations` / `messages` subcollection — direct messaging (participants array gates access); bot conversations use fixed IDs for `__journeyhub_platform_guide__` and `__journeyhub_spark_bot__`
- `events` — community events with RSVP via attendees
- `notifications` — per-user with read status
- `reports` — content reports (write-only from client)

Growth & social:
- `growthPaths` + `members`, `updates` subcollections — milestone-based goals with accountability
- `journeyResponses` — responses to profile life-journey prompts
- `feedPosts` — following-feed posts
- `investments` — "invest in person" mechanic
- `shoutouts` — peer recognition
- `trustVouches` — endorsements
- `mentorships` — mentorship connections
- `missions` + `comments` subcollection — structured challenges
- `thoughtLab` — shared ideas
- `coThinkSessions` — collaborative thinking sessions

AI/analytics caches:
- `communityInsights`, `memberProfiles`, `topicAnalysis`, `keystoneMembers` (+ `members` subcollection) — AI analysis outputs
- `digests` — generated weekly digests
- `cache` — generic response cache

Infrastructure:
- `analyticsEvents`, `rateLimits`, `invites`, `tags`, `tagSubscriptions` (+ `tags` subcollection), `shareProfiles`, `userBadges` (+ `badges` subcollection)

## Common Commands

```bash
# Deploy everything to Firebase (hosting, rules, indexes, functions)
firebase deploy

# Deploy only hosting
firebase deploy --only hosting

# Deploy only Firestore rules
firebase deploy --only firestore:rules

# Deploy only Cloud Functions
firebase deploy --only functions

# Run Firebase emulators locally
firebase emulators:start

# View deployed site
firebase open hosting:site

# Railway deploy (alternate path — static only, uses shims for AI)
# Pushed via Railway's Git integration; runs `npm start` → server.js
```

The Cloud Functions in `functions/index.js` require `npm install` in the `functions/` directory before deploying (Firebase CLI handles this during `firebase deploy --only functions`).

## Key Patterns

- **No routing library** — navigation is handled by `show*()` functions (e.g., `showHome()`, `showThread(id)`, `showProfile(uid)`, `showGrowthPaths()`, `showMessages()`) that replace `#app` innerHTML.
- **Real-time updates** — `onSnapshot` listeners for threads, comments, messages, notifications, co-think sessions. Active listeners are tracked on module-level `*Listener` variables and detached on view changes.
- **Auth flow** — Firebase Auth (email/password + Google). `onAuthStateChanged` in `init()` drives state. Username is derived from email prefix.
- **All handlers exposed on `window`** — event handlers referenced in inline HTML `onclick=` attributes must be assigned to `window.*` at the bottom of the script block.
- **Image uploads** — Firebase Storage via `uploadImage()`, returning a download URL stored on the Firestore doc.
- **AI bot conversations** — the JourneyHub Guide and Spark Bot have fixed user IDs; messages sent in their conversations trigger `onDocumentCreated` Cloud Functions (`replyToPlatformGuideMessage`, `replyToSparkBotMessage`) that call Gemini and write the reply back to the same conversation.
- **Shim compatibility** — when adding new Cloud Function calls, consider whether `shims/cloud-functions.js` needs a browser-side equivalent for the Railway build.
