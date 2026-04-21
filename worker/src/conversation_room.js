// Durable Object: one instance per conversation id. Holds WebSocket connections
// for participants and broadcasts change events (which clients turn into a
// refetch from D1). Uses the Hibernation API so idle connections cost nothing.

export class ConversationRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }

  async fetch(request) {
    if (request.headers.get('Upgrade') !== 'websocket') {
      return new Response('Expected WebSocket', { status: 426 });
    }
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  webSocketMessage(ws, message) {
    if (message === 'ping') {
      try { ws.send('pong'); } catch {}
    }
  }

  webSocketClose(_ws, _code, _reason, _wasClean) {
    // no-op — hibernation lifecycle manages the session set via ctx.getWebSockets()
  }

  webSocketError(_ws, _error) {
    // no-op
  }

  // RPC: called by the main Worker after a message is written to D1.
  async broadcast(payload) {
    const msg = typeof payload === 'string' ? payload : JSON.stringify(payload);
    for (const ws of this.ctx.getWebSockets()) {
      try { ws.send(msg); } catch {}
    }
  }
}
