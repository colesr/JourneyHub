// firebase-storage.js — Cloudflare R2 backed shim of the v9 modular Storage API.
// Uploads POST the file to /api/upload?path=<path> with the blob's content type;
// the Worker writes to R2 under <path>. Downloads resolve to "/r2/<path>" which
// the Worker streams from R2 (public read, long cache).

const MAX_FILE_BYTES = 5 * 1024 * 1024;

class StorageReference {
  constructor(path) {
    this.fullPath = path;
    this.name = path.split('/').pop();
    this.bucket = 'journeyhub-images';
  }
}

export function getStorage(_app) {
  return { __isStorage: true };
}

export function ref(_storage, path) {
  return new StorageReference(path);
}

export async function uploadBytes(storageRef, fileOrBlob, _metadata) {
  const size = fileOrBlob && fileOrBlob.size != null ? fileOrBlob.size : 0;
  if (size > MAX_FILE_BYTES) {
    const err = new Error(`storage/file-too-large: ${(size / 1024 / 1024).toFixed(1)} MB, max is 5 MB`);
    err.code = 'storage/file-too-large';
    throw err;
  }
  const contentType = fileOrBlob.type || 'application/octet-stream';
  const res = await fetch('/api/upload?path=' + encodeURIComponent(storageRef.fullPath), {
    method: 'POST',
    credentials: 'include',
    headers: { 'Content-Type': contentType },
    body: fileOrBlob,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(data.error || `storage/upload-failed: HTTP ${res.status}`);
    err.code = 'storage/upload-failed';
    throw err;
  }
  return {
    ref: storageRef,
    metadata: {
      fullPath: storageRef.fullPath,
      name: storageRef.name,
      size,
      contentType,
    },
  };
}

export async function getDownloadURL(storageRef) {
  return '/r2/' + storageRef.fullPath;
}

export async function deleteObject(storageRef) {
  const res = await fetch('/api/upload?path=' + encodeURIComponent(storageRef.fullPath), {
    method: 'DELETE',
    credentials: 'include',
  });
  if (!res.ok && res.status !== 404) {
    const err = new Error(`storage/delete-failed: HTTP ${res.status}`);
    err.code = 'storage/delete-failed';
    throw err;
  }
}

export async function uploadString(storageRef, value, format) {
  let body;
  let contentType = 'text/plain';
  if (format === 'data_url' && typeof value === 'string' && value.startsWith('data:')) {
    const match = value.match(/^data:([^;,]+)(?:;base64)?,(.*)$/);
    if (match) {
      contentType = match[1];
      const isBase64 = value.includes(';base64,');
      if (isBase64) {
        const bin = atob(match[2]);
        const bytes = new Uint8Array(bin.length);
        for (let i = 0; i < bin.length; i++) bytes[i] = bin.charCodeAt(i);
        body = bytes.buffer;
      } else {
        body = decodeURIComponent(match[2]);
      }
    } else {
      body = value;
    }
  } else {
    body = value;
  }
  const blob = body instanceof ArrayBuffer ? new Blob([body], { type: contentType }) : new Blob([body], { type: contentType });
  return uploadBytes(storageRef, blob);
}
