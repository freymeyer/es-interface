const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const WebSocket = require('ws');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');

const app = express();
const server = http.createServer(app);
const io = new Server(server);

const PORT = process.env.PORT || 3000;
const STATE_FILE = path.join(__dirname, 'state.json');

// OpenClaw Gateway settings
const GATEWAY_URL = process.env.OPENCLAW_GATEWAY_URL;
const GATEWAY_TOKEN = process.env.OPENCLAW_GATEWAY_TOKEN;
const AGENT_NAME = process.env.OPENCLAW_AGENT || 'es';

// ─────────────────────────────────────────────────────────────────────────────
// STATE
// ─────────────────────────────────────────────────────────────────────────────

let state = {
    messages: [],
    currentExpression: 'passive_eyes_mouth_closed.png',
    currentModel: null,
};

if (fs.existsSync(STATE_FILE)) {
    try {
        state = JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
    } catch (e) {
        console.error('Failed to load state.json:', e);
    }
}

function saveState() {
    fs.writeFileSync(STATE_FILE, JSON.stringify(state, null, 2));
}

function pushState(type, data) {
    const entry = { type, data, timestamp: new Date().toISOString() };
    if (['message', 'user_message', 'thought'].includes(type)) {
        state.messages.push(entry);
        if (state.messages.length > 200) state.messages.shift();
    } else if (type === 'expression') {
        state.currentExpression = data;
    } else if (type === 'model_update') {
        state.currentModel = data;
    }
    saveState();
}

// ─────────────────────────────────────────────────────────────────────────────
// GATEWAY RAW WEBSOCKET CLIENT
// ─────────────────────────────────────────────────────────────────────────────

let ws = null;
let wsConnected = false;
let wsReady = false;
let reqId = 1;
const pendingReqs = {};
let messageQueue = [];
let reconnectTimer = null;

// ── STREAMING STATE ──────────────────────────────────────────────────────────
let streamingMessageId = null;
let latestAgentText = '';

// ── PRESENCE STATE ──────────────────────────────────────────────────────────
let presenceNodes = {}; // deviceId → presence entry

function nextId() { return String(reqId++); }

function wsSend(obj) {
    if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(obj));
        return true;
    }
    return false;
}

function gatewayRequest(method, params = {}) {
    return new Promise((resolve, reject) => {
        const id = nextId();
        const timeout = setTimeout(() => {
            delete pendingReqs[id];
            reject(new Error(`Gateway request "${method}" timed out`));
        }, 120_000);
        pendingReqs[id] = { resolve, reject, timeout };
        if (!wsSend({ type: 'req', id, method, params })) {
            clearTimeout(timeout);
            delete pendingReqs[id];
            reject(new Error('Gateway not connected'));
        }
    });
}

function connectToGateway() {
    if (reconnectTimer) { clearTimeout(reconnectTimer); reconnectTimer = null; }

    console.log(`[Gateway] Connecting to ${GATEWAY_URL} ...`);
    io.emit('update', { type: 'gateway_status', data: 'connecting' });
    io.emit('update', { type: 'queue_count', data: messageQueue.length });

    ws = new WebSocket(GATEWAY_URL, {
        headers: {
            Origin: 'http://localhost:3000'
        }
    });

    ws.on('open', () => {
        wsConnected = true;
        console.log('[Gateway] Socket open — waiting for connect.challenge ...');
    });

    ws.on('message', (raw) => {
        let msg;
        try { msg = JSON.parse(raw); }
        catch (e) { console.warn('[Gateway] Non-JSON frame'); return; }

        if (msg.type === 'event' && msg.event === 'connect.challenge') {
            console.log('[Gateway] Got connect.challenge, sending connect req ...');
            const connectParams = {
                minProtocol: 3,
                maxProtocol: 3,
                client: {
                     id: 'openclaw-control-ui',
                    version: '1.0.0',
                    platform: process.platform,
                    mode: 'webchat'
                },
                locale: 'en-US',
                userAgent: 'amadeus-hud/2.0',
                auth: {
                    token: GATEWAY_TOKEN,   // gateway token
                },
                role: 'operator',
                scopes: ['operator.admin', 'operator.write', 'operator.approvals', 'operator.pairing']
                };
            wsSend({ type: 'req', id: nextId(), method: 'connect', params: connectParams });
            return;
        }

        if (msg.type === 'res') {
            if (!wsReady) {
                if (msg.ok) {
                    wsReady = true;
                    console.log('[Gateway] Handshake OK ✓');
                    console.log("CONNECT RESPONSE:", JSON.stringify(msg.payload, null, 2));
                    io.emit('update', { type: 'gateway_status', data: 'connected' });
                    io.emit('update', { type: 'queue_count', data: messageQueue.length });
                    // Fetch initial presence
                    gatewayRequest('system-presence').then(p => {
                        if (p) handlePresencePayload(p);
                    }).catch(() => {});
                    // Flush queued messages
                    while (messageQueue.length > 0) {
                        const { message, attachments, resolve, reject } = messageQueue.shift();
                        runAgentMessage(message, attachments, resolve, reject);
                        io.emit('update', { type: 'queue_count', data: messageQueue.length });
                    }
                } else {
                    console.error('[Gateway] Handshake failed:', msg.error);
                    io.emit('update', { type: 'ERROR', data: `Handshake failed: ${msg.error?.message || 'unknown'}` });
                    ws.close();
                }
                return;
            }
            const pending = pendingReqs[msg.id];
            if (pending) {
                clearTimeout(pending.timeout);
                delete pendingReqs[msg.id];
                if (msg.ok) pending.resolve(msg.payload);
                else pending.reject(new Error(msg.error?.message || 'Gateway error'));
            }
            return;
        }

        if (msg.type === 'event') {

            // 🔐 DEVICE PAIR REQUEST HANDLER
            if (msg.event === 'device.pair.requested') {
                console.log('\n=== DEVICE PAIR REQUESTED ===');
                console.log(JSON.stringify(msg.payload, null, 2));
                console.log('Run: openclaw devices list');
                console.log('Then: openclaw devices approve <requestId>');
                console.log('================================\n');
                return; // stop further handling
            }

            if (!['heartbeat', 'tick'].includes(msg.event)) {
                console.log(
                    `[Gateway] EVENT: ${msg.event}`,
                    JSON.stringify(msg.payload)?.substring(0, 200)
                );
            }

            handleGatewayEvent(msg.event, msg.payload);
        }
    });

    ws.on('close', (code) => {
        wsConnected = false;
        wsReady = false;
        console.warn(`[Gateway] Disconnected (${code})`);
        io.emit('update', { type: 'gateway_status', data: 'disconnected' });
        io.emit('update', { type: 'presence_update', data: [] });
        for (const [id, p] of Object.entries(pendingReqs)) {
            clearTimeout(p.timeout);
            p.reject(new Error('Gateway disconnected'));
            delete pendingReqs[id];
        }
        reconnectTimer = setTimeout(connectToGateway, 3000);
    });

    ws.on('error', (err) => {
        console.error('[Gateway] Error:', err.message);
        io.emit('update', { type: 'ERROR', data: `Gateway error: ${err.message}` });
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// PRESENCE HELPERS
// ─────────────────────────────────────────────────────────────────────────────

function handlePresencePayload(payload) {
    // payload is array or object keyed by deviceId
    if (Array.isArray(payload)) {
        presenceNodes = {};
        payload.forEach(entry => {
            const key = entry.deviceId || entry.id || entry.client?.id || JSON.stringify(entry).substring(0, 16);
            presenceNodes[key] = entry;
        });
    } else if (typeof payload === 'object') {
        presenceNodes = payload;
    }
    io.emit('update', { type: 'presence_update', data: Object.values(presenceNodes) });
}

// ─────────────────────────────────────────────────────────────────────────────
// GATEWAY EVENT HANDLER
// ─────────────────────────────────────────────────────────────────────────────


function sanitizeForDisplay(rawText) {
    const strippedMedia = (rawText || '').replace(/MEDIA:\/\S+/g, '').trim();
    if (!strippedMedia) return '';

    return strippedMedia
        .replace(/<\s*ja\s*>[\s\S]*?<\s*\/\s*ja\s*>/gi, '')
        .trim();
}

function handleGatewayEvent(event, payload) {
    if (!event) return;
    if (['heartbeat', 'health', 'tick', 'connect.challenge'].includes(event)) return;

    if (event === 'agent') { handleAgentEvent(payload); return; }
    if (event === 'chat')  { handleChatEvent(payload);  return; }

    // ── PRESENCE ─────────────────────────────────────────────────────────────
    if (event === 'presence') {
        handlePresencePayload(Array.isArray(payload) ? payload : [payload]);
        return;
    }

    // ── EXEC APPROVAL ────────────────────────────────────────────────────────
    if (event === 'exec.approval.requested') {
        console.log('[Gateway] Exec approval requested:', payload);
        io.emit('update', {
            type: 'exec_approval',
            data: {
                id: payload?.id || payload?.requestId,
                command: payload?.command || payload?.args?.command || JSON.stringify(payload?.args || {}),
                tool: payload?.tool || 'exec',
                risk: payload?.risk || 'medium',
            }
        });
        return;
    }
}

function handleAgentEvent(payload) {
    if (!payload) return;

    const stream = payload.stream;
    const data   = payload.data;

    if (stream === 'lifecycle') {
        if (data?.phase === 'start') {
            latestAgentText = '';
            streamingMessageId = crypto.randomBytes(4).toString('hex');
            io.emit('update', { type: 'thinking', data: true });
            io.emit('update', { type: 'stream_start', data: { id: streamingMessageId } });
        } else if (data?.phase === 'end') {
            io.emit('update', { type: 'thinking', data: false });
            io.emit('update', { type: 'stream_end', data: { id: streamingMessageId } });
            streamingMessageId = null;
            if (data?.usage)            io.emit('update', { type: 'usage',        data: data.usage });
            if (data?.model)            { io.emit('update', { type: 'model_update', data: data.model }); pushState('model_update', data.model); }
            if (data?.agentMeta?.usage) io.emit('update', { type: 'usage',        data: data.agentMeta.usage });
            if (data?.agentMeta?.model) { io.emit('update', { type: 'model_update', data: data.agentMeta.model }); pushState('model_update', data.agentMeta.model); }
        }
        return;
    }

    if (stream === 'assistant' && data?.text) {
        const newText = data.text;
        const chunk   = newText.slice(latestAgentText.length);
        latestAgentText = newText;

        if (chunk && streamingMessageId) {
            io.emit('update', {
                type: 'stream_chunk',
                data: { id: streamingMessageId, chunk, full: sanitizeForDisplay(newText) }
            });
        }
        return;
    }

    if (stream === 'tool' || stream === 'tool_start') {
        const toolName = data?.name || data?.tool || 'unknown';
        let cmd = data?.args ? JSON.stringify(data.args) : '';
        if (toolName === 'exec'  && data?.args?.command) cmd = data.args.command;
        if (toolName === 'read'  && data?.args?.path)    cmd = `read ${data.args.path}`;
        if (toolName === 'write' && data?.args?.path)    cmd = `write ${data.args.path}`;
        io.emit('update', { type: 'system_action', data: { tool: toolName, command: cmd } });
        return;
    }

    if (stream === 'thinking' && data?.text) {
        io.emit('update', { type: 'thought', data: data.text });
        pushState('thought', data.text);
        return;
    }
}

function handleChatEvent(payload) {
    if (!payload) return;
    if (payload.state !== 'final') return;

    // Try latestAgentText first (populated by assistant stream chunks)
    let replyText = latestAgentText;

    // Fallback: extract from payload.message.content
    if (!replyText) {
        const content = payload.message?.content;
        if (typeof content === 'string') {
            replyText = content;
        } else if (Array.isArray(content)) {
            replyText = content.filter(b => b.type === 'text').map(b => b.text).join('');
        }
    }

    // NEW: also check payload.text directly (some gateway versions put it here)
    if (!replyText && payload.text) {
        replyText = payload.text;
    }

    // NEW: check result.content or result.text
    if (!replyText && payload.result) {
        const r = payload.result;
        if (typeof r === 'string') replyText = r;
        else if (r?.text) replyText = r.text;
        else if (Array.isArray(r?.content)) {
            replyText = r.content.filter(b => b.type === 'text').map(b => b.text).join('');
        }
    }

    console.log('[DEBUG] handleChatEvent full payload:', JSON.stringify(payload, null, 2));

    if (!replyText) {
        console.warn('[Chat] Final event but no text found anywhere in payload');
        latestAgentText = '';
        return;
    }
    if (!replyText) return;

    const thoughtMatch = replyText.match(/<thinking>([\s\S]*?)<\/thinking>/);
    if (thoughtMatch) {
        io.emit('update', { type: 'thought', data: thoughtMatch[1].trim() });
        pushState('thought', thoughtMatch[1].trim());
        replyText = replyText.replace(thoughtMatch[0], '').trim();
    }

    let expression = null;
    const exprMatch = replyText.match(/\[expressions\/([^\]]+\.png)\]/);
    if (exprMatch) {
        expression = `expressions/${exprMatch[1]}`;
        replyText = replyText.replace(exprMatch[0], '').trim();
    }


    if (replyText) {
        const displayText = sanitizeForDisplay(replyText);

        console.log('[DEBUG] display:', displayText);

        if (displayText) {
            pushState('message', displayText);
            io.emit('update', { type: 'vn_text', data: displayText });
        }
    }

        if (expression) {
            io.emit('update', { type: 'expression', data: expression });
            pushState('expression', expression);
        }

    latestAgentText = '';
}

function processAgentResult(response) {
    try {
        const agentMeta = response?.result?.meta?.agentMeta
            || response?.meta?.agentMeta
            || response?.agentMeta;
        if (agentMeta?.usage) io.emit('update', { type: 'usage',        data: agentMeta.usage });
        if (agentMeta?.model) { io.emit('update', { type: 'model_update', data: agentMeta.model }); pushState('model_update', agentMeta.model); }
    } catch (err) {
        console.error('[Gateway] processAgentResult error:', err);
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SEND MESSAGE TO GATEWAY
// ─────────────────────────────────────────────────────────────────────────────

function buildMessageContent(text, attachments) {
    if (!attachments || attachments.length === 0) return text;

    // Build multi-part content with images
    const parts = [];
    for (const att of attachments) {
        if (att.type === 'image') {
            parts.push({
                type: 'image',
                source: { type: 'base64', media_type: att.mimeType, data: att.data }
            });
        } else if (att.type === 'file') {
            parts.push({ type: 'text', text: `[Attached file: ${att.name}]\n\`\`\`\n${att.data}\n\`\`\`` });
        }
    }
    parts.push({ type: 'text', text });
    return parts;
}

function runAgentMessage(message, attachments, resolve, reject) {
    const sessionKey = `agent:${AGENT_NAME}:hud`;
    const content = buildMessageContent(message, attachments);

    gatewayRequest('chat.send', {
        sessionKey,
        message: content,
        idempotencyKey: crypto.randomBytes(8).toString('hex'),
    })
    .then((result) => {
        console.log('[Gateway] chat.send res payload:', JSON.stringify(result)?.substring(0, 300));
        if (result) {
            io.emit('update', { type: 'thinking', data: false });
            processAgentResult(result);
        }
        resolve(result);
    })
    .catch((err) => {
        console.error('[Gateway] chat.send error:', err.message);
        io.emit('update', { type: 'thinking', data: false });
        io.emit('update', { type: 'ERROR', data: err.message });
        reject(err);
    });
}

async function handleOutboundMessage(payload) {
    const message = typeof payload === 'string' ? payload : (payload.message || '');
    const attachments = payload.attachments || [];
    if (!message) return;

    console.log(`[HUD] → "${message.substring(0, 80)}"`);
    pushState('user_message', message);

    // Build display text with attachment indicators
    let displayText = message;
    if (attachments.length > 0) {
        const attInfo = attachments.map(a => `[📎 ${a.name}]`).join(' ');
        displayText = `${attInfo}\n${message}`.trim();
    }

    io.emit('update', { type: 'user_message', data: displayText });
    io.emit('update', { type: 'thinking', data: true });

    return new Promise((resolve, reject) => {
        if (!wsReady) {
            messageQueue.push({ message, attachments, resolve, reject });
            io.emit('update', { type: 'queue_count', data: messageQueue.length });
        } else {
            runAgentMessage(message, attachments, resolve, reject);
        }
    });
}

// ─────────────────────────────────────────────────────────────────────────────
// MEMORY
// ─────────────────────────────────────────────────────────────────────────────

const MEMORY_FILE = path.join(process.env.HOME || '/tmp', '.openclaw', 'MEMORY.md');

function appendToMemory(content, tag) {
    try {
        const dir = path.dirname(MEMORY_FILE);
        if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
        const ts = new Date().toISOString();
        const entry = `\n\n---\n<!-- saved ${ts} -->\n${tag ? `**[${tag}]** ` : ''}${content}\n`;
        fs.appendFileSync(MEMORY_FILE, entry);
        return true;
    } catch (e) {
        console.error('[Memory] Failed to write:', e.message);
        return false;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// EXPRESS ROUTES
// ─────────────────────────────────────────────────────────────────────────────

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json({ limit: '50mb' }));

app.post('/api/update', (req, res) => {
    if (req.headers['x-api-key'] !== API_KEY)
        return res.status(401).json({ status: 'error', message: 'Unauthorized' });
    const { type, data } = req.body;
    pushState(type, data);
    io.emit('update', { type, data });
    res.json({ status: 'ok' });
});

app.post('/api/message', async (req, res) => {
    const { message, attachments } = req.body;
    if (!message) return res.status(400).json({ error: 'Message required' });
    try { await handleOutboundMessage({ message, attachments }); res.json({ status: 'ok' }); }
    catch (err) { res.status(500).json({ status: 'error', message: err.message }); }
});

app.post('/api/session/reset', (req, res) => {
    state.messages = [];
    saveState();
    io.emit('update', { type: 'session_reset' });
    if (wsReady) {
        gatewayRequest('sessions.reset', { key: `agent:${AGENT_NAME}:hud` })
            .catch((e) => console.warn('[Gateway] Session reset warn:', e.message));
    }
    io.emit('update', { type: 'INFO', data: 'Session reset.' });
    res.json({ status: 'ok' });
});

// ── EXEC APPROVAL ─────────────────────────────────────────────────────────
app.post('/api/exec/approve', async (req, res) => {
    const { id, approved } = req.body;
    if (!id) return res.status(400).json({ error: 'id required' });
    if (!wsReady) return res.status(503).json({ error: 'Gateway not connected' });
    try {
        await gatewayRequest('exec.approval.resolve', { id, approved: !!approved });
        io.emit('update', { type: 'exec_approval_resolved', data: { id, approved: !!approved } });
        res.json({ status: 'ok' });
    } catch (err) {
        res.status(500).json({ status: 'error', message: err.message });
    }
});

// ── MEMORY SAVE ──────────────────────────────────────────────────────────
app.post('/api/memory/save', (req, res) => {
    const { content, tag } = req.body;
    if (!content) return res.status(400).json({ error: 'content required' });
    const ok = appendToMemory(content, tag);
    if (ok) {
        io.emit('update', { type: 'INFO', data: `Saved to MEMORY.md` });
        res.json({ status: 'ok' });
    } else {
        res.status(500).json({ status: 'error', message: 'Failed to write memory' });
    }
});

// ── MODEL SWITCH ─────────────────────────────────────────────────────────
app.post('/api/model/set', async (req, res) => {
    const { model } = req.body;
    if (!model) return res.status(400).json({ error: 'model required' });
    if (!wsReady) return res.status(503).json({ error: 'Gateway not connected' });
    try {
        // Try agent config update or config.set depending on gateway version
        await gatewayRequest('config.set', { path: `agents.${AGENT_NAME}.model`, value: model });
        state.currentModel = model;
        saveState();
        io.emit('update', { type: 'model_update', data: model });
        io.emit('update', { type: 'INFO', data: `Model set to ${model}` });
        res.json({ status: 'ok', model });
    } catch (err) {
        // Fallback: just update local state
        state.currentModel = model;
        saveState();
        io.emit('update', { type: 'model_update', data: model });
        io.emit('update', { type: 'INFO', data: `Model preference set to ${model} (local)` });
        res.json({ status: 'ok', model, note: 'local only' });
    }
});

// ── PRESENCE ─────────────────────────────────────────────────────────────
app.get('/api/presence', async (req, res) => {
    if (!wsReady) return res.json({ nodes: [] });
    try {
        const p = await gatewayRequest('system-presence');
        if (p) handlePresencePayload(p);
        res.json({ nodes: Object.values(presenceNodes) });
    } catch (e) {
        res.json({ nodes: Object.values(presenceNodes) });
    }
});

// ── HISTORY (paginated) ───────────────────────────────────────────────────
app.get('/api/history', (req, res) => {
    const page = parseInt(req.query.page) || 0;
    const limit = parseInt(req.query.limit) || 50;
    const all = state.messages || [];
    const start = Math.max(0, all.length - limit - page * limit);
    const end = all.length - page * limit;
    res.json({
        messages: all.slice(start, end),
        hasMore: start > 0,
        total: all.length
    });
});

app.get('/api/gateway/status', (_req, res) => {
    res.json({
        connected: wsReady,
        url: GATEWAY_URL,
        agent: AGENT_NAME,
        queue: messageQueue.length,
        model: state.currentModel,
        nodes: Object.values(presenceNodes).length,
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// SOCKET.IO — HUD CLIENTS
// ─────────────────────────────────────────────────────────────────────────────

io.on('connection', (socket) => {
    console.log('[HUD] Client connected');
    socket.emit('init_state', state);
    socket.emit('update', { type: 'gateway_status', data: wsReady ? 'connected' : 'disconnected' });
    socket.emit('update', { type: 'queue_count', data: messageQueue.length });
    if (Object.keys(presenceNodes).length > 0) {
        socket.emit('update', { type: 'presence_update', data: Object.values(presenceNodes) });
    }
    if (state.currentModel) {
        socket.emit('update', { type: 'model_update', data: state.currentModel });
    }

    socket.on('chat_message', async (payload) => {
        try { await handleOutboundMessage(payload); }
        catch (err) { socket.emit('update', { type: 'ERROR', data: err.message }); }
    });

    socket.on('exec_approve', async ({ id, approved }) => {
        if (!wsReady) return;
        try {
            await gatewayRequest('exec.approval.resolve', { id, approved: !!approved });
            io.emit('update', { type: 'exec_approval_resolved', data: { id, approved: !!approved } });
        } catch (err) {
            socket.emit('update', { type: 'ERROR', data: err.message });
        }
    });

    socket.on('ping', () => socket.emit('pong'));
});

// ─────────────────────────────────────────────────────────────────────────────
// BOOT
// ─────────────────────────────────────────────────────────────────────────────

server.listen(PORT, () => {
    console.log(`\n╔══════════════════════════════════════╗`);
    console.log(`║  Amadeus Interface v2.1              ║`);
    console.log(`║  HUD  → http://localhost:${PORT}       ║`);
    console.log(`║  Gate → ${GATEWAY_URL.padEnd(28)}║`);
    console.log(`║  Auth → ${(GATEWAY_TOKEN ? 'token set ✓' : 'NO TOKEN SET').padEnd(28)}║`);
    console.log(`╚══════════════════════════════════════╝\n`);
    connectToGateway();
});
