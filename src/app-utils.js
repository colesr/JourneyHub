import { RESOURCE_LIBRARY } from './app-config.js';

export function buildDefaultProfileData(email = '', serverTimestamp) {
  const username = email.split('@')[0] || 'member';
  return {
    username,
    usernameLower: username.toLowerCase(),
    email,
    headline: '',
    bio: '',
    location: '',
    website: '',
    statusNote: '',
    currentFocus: '',
    growthGoal: '',
    growthChallenge: '',
    growthStage: '',
    preferredSupport: '',
    lookingFor: '',
    lifeJourney: '',
    interests: [],
    expertiseAreas: [],
    skills: [],
    askMeAbout: [],
    projectsText: '',
    favoriteQuote: '',
    profilePicture: '',
    joinDate: serverTimestamp(),
    updatedAt: null,
    reputation: 0,
    badges: [],
    followers: [],
    following: [],
    mutedUsers: [],
    blockedUsers: [],
    savedResources: [],
    joinedPaths: [],
    onboardingComplete: false,
    onboardingSignalsSet: false,
  };
}

export function normalizeUsernameSeed(value = '') {
  const cleaned = (value || '')
    .toLowerCase()
    .replace(/[^a-z0-9_.-]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 24);
  return cleaned || 'member';
}

export function parseCommaSeparated(value) {
  return (value || '').split(',').map((s) => s.trim()).filter(Boolean);
}

export function normalizeUrl(value) {
  const url = (value || '').trim();
  if (!url) return '';
  if (/^https?:\/\//i.test(url)) return url;
  return `https://${url}`;
}

export function excerpt(text, limit = 180) {
  const content = (text || '').trim();
  if (!content) return '';
  return content.length > limit ? `${content.slice(0, limit).trim()}...` : content;
}

export function extractMentions(text) {
  const matches = (text || '').match(/@([a-zA-Z0-9_.-]+)/g) || [];
  return [...new Set(matches.map((match) => match.slice(1).toLowerCase()))];
}

export function renderTextWithMentions(text) {
  return escapeHtml(text || '').replace(/(^|\s)@([a-zA-Z0-9_.-]+)/g, (_match, prefix, username) => {
    return `${prefix}<a href="#" onclick="window.openProfileByUsername('${escapeAttr(username.toLowerCase())}'); return false;">@${escapeHtml(username)}</a>`;
  });
}

export function timestampToMillis(timestamp) {
  if (!timestamp) return 0;
  if (timestamp.toMillis) return timestamp.toMillis();
  if (timestamp.toDate) return timestamp.toDate().getTime();
  return new Date(timestamp).getTime() || 0;
}

export function getResourceById(resourceId) {
  return RESOURCE_LIBRARY.find((resource) => resource.id === resourceId) || null;
}

export function escapeHtml(text) {
  if (!text) return '';
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

export function escapeAttr(text) {
  if (!text) return '';
  return text.replace(/&/g, '&amp;').replace(/'/g, '&#39;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

export function formatTime(timestamp) {
  if (!timestamp) return 'just now';
  const date = timestamp.toDate ? timestamp.toDate() : new Date(timestamp);
  const now = new Date();
  const diff = now - date;
  const minutes = Math.floor(diff / 60000);
  const hours = Math.floor(diff / 3600000);
  const days = Math.floor(diff / 86400000);
  if (minutes < 1) return 'just now';
  if (minutes < 60) return `${minutes}m ago`;
  if (hours < 24) return `${hours}h ago`;
  return `${days}d ago`;
}
