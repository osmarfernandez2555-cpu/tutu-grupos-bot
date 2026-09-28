// ─────────────────────────────────────────────────────────────────────────────
// TUTU GRUPOS — bot que escucha grupos de WhatsApp (vía Evolution API), detecta
// avisos de autos en venta con Claude y los carga solos en el stock de Ruthina.
// NUNCA envía mensajes: solo lee. Eso reduce mucho el riesgo de bloqueo del número.
// ─────────────────────────────────────────────────────────────────────────────
require('dotenv').config();
const express  = require('express');
const cors     = require('cors');
const path     = require('path');
const crypto   = require('crypto');
const Database = require('better-sqlite3');

// ── Configuración (todo por variables de entorno; no hay claves en el código) ─
const PORT         = process.env.PORT || 3000;
const EVO_URL      = (process.env.EVO_URL || '').replace(/\/$/, '');
const EVO_APIKEY   = process.env.EVO_APIKEY || '';
const EVO_INSTANCE = process.env.EVO_INSTANCE || '';
const RUTHINA_URL  = (process.env.RUTHINA_URL || 'https://compara-conejo-production.up.railway.app').replace(/\/$/, '');
const IMPORT_KEY   = process.env.IMPORT_KEY || '';
const ADMIN_SECRET = process.env.ADMIN_SECRET || '';
const MODEL        = process.env.MODEL || 'claude-haiku-4-5-20251001';
const ANTHROPIC_URL = process.env.ANTHROPIC_URL || 'https://api.anthropic.com/v1/messages';
const DB_PATH      = process.env.DB_PATH || path.join(__dirname, 'grupos.db');
const ACTIVE_GROUPS = (process.env.ACTIVE_GROUPS || '').split(',').map(s => s.trim()).filter(Boolean);
const TTL_MS       = 7 * 24 * 60 * 60 * 1000; // memoria de mensajes ya vistos
const MAX_COLA     = 200;

const sleep = ms => new Promise(r => setTimeout(r, ms));
const sha1  = s => crypto.createHash('sha1').update(String(s)).digest('hex');

// ── Base de datos local (SQLite) ──────────────────────────────────────────────
const db = new Database(DB_PATH);
db.pragma('journal_mode = WAL');
db.exec(`
  CREATE TABLE IF NOT EXISTS grupos (
    jid TEXT PRIMARY KEY, nombre TEXT DEFAULT '', activo INTEGER NOT NULL DEFAULT 0,
    primer_visto INTEGER, ultimo_msg INTEGER, msgs_total INTEGER DEFAULT 0, autos_cargados INTEGER DEFAULT 0
  );
  CREATE TABLE IF NOT EXISTS vistos (clave TEXT PRIMARY KEY, ts INTEGER NOT NULL);
  CREATE TABLE IF NOT EXISTS log_autos (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER, grupo TEXT, vendedor TEXT, telefono TEXT,
    marca TEXT, modelo TEXT, anio TEXT, precio TEXT, accion TEXT, error TEXT
  );
`);
// Grupos que queden activos aunque la base se borre (por si no tenés Volume en Railway)
for (const jid of ACTIVE_GROUPS) {
  db.prepare('INSERT OR IGNORE INTO grupos (jid, activo, primer_visto) VALUES (?,1,?)').run(jid, Date.now());
  db.prepare('UPDATE grupos SET activo = 1 WHERE jid = ?').run(jid);
}

function yaVisto(clave) {
  const r = db.prepare('SELECT ts FROM vistos WHERE clave = ?').get(clave);
  return !!(r && Date.now() - r.ts < TTL_MS);
}
function marcarVisto(clave) {
  db.prepare('INSERT OR REPLACE INTO vistos (clave, ts) VALUES (?,?)').run(clave, Date.now());
}
function logAuto(d) {
  db.prepare(`INSERT INTO log_autos (ts, grupo, vendedor, telefono, marca, modelo, anio, precio, accion, error)
              VALUES (?,?,?,?,?,?,?,?,?,?)`)
    .run(Date.now(), d.grupo || '', d.vendedor || '', d.telefono || '', d.marca || '', d.modelo || '',
         d.anio || '', d.precio || '', d.accion || '', d.error || '');
}
setInterval(() => {
  try {
    db.prepare('DELETE FROM vistos WHERE ts < ?').run(Date.now() - TTL_MS);
    db.prepare('DELETE FROM log_autos WHERE id <= (SELECT COALESCE(MAX(id),0) FROM log_autos) - 1000').run();
  } catch (e) { console.error('[LIMPIEZA]', e.message); }
}, 60 * 60 * 1000).unref();

// ── Filtro barato antes de gastar una llamada a Claude ───────────────────────
// Un aviso real casi siempre trae al menos 2 de estas 3 cosas: año, precio, km.
function pareceAviso(t) {
  if (!t || t.length < 20 || t.length > 8000) return false;
  const anio   = /\b(19[789]\d|20[0-3]\d)\b/.test(t);
  const precio = /(\$|u\$s|us\$|usd|d[oó]lar|mill[oó]n|palos|\b\d{1,3}(\.\d{3}){1,2}\b|\b\d{5,9}\b)/i.test(t);
  const km     = /\bkm\b|\bkms\b|kil[oó]metro/i.test(t);
  return [anio, precio, km].filter(Boolean).length >= 2;
}
const normalizarTexto = t => String(t).toLowerCase().replace(/\s+/g, ' ').trim();

// ── Claude: extracción de avisos ─────────────────────────────────────────────
const SYSTEM_EXTRACTOR = `Sos un extractor de avisos de venta de autos usados que se publican en grupos de WhatsApp de concesionarias y agencias de Argentina.
Devolvé SOLO un JSON array, sin texto adicional ni markdown. Cada elemento es un vehículo (auto, camioneta o utilitario) ofrecido EN VENTA:
{"marca":"","modelo":"","version":"","anio":"","km":"","precio":"","moneda":"ARS","color":"","telefono":"","notas":""}

Reglas:
- Si el mensaje NO ofrece vehículos en venta (charla, saludos, consultas, pedidos de compra como "busco", "necesito", "compro", "se busca", publicidad de servicios, repuestos, motos, camiones pesados) devolvé [].
- Un mensaje puede traer varios autos: un elemento por cada uno.
- marca: si no la dice pero es inequívoca por el modelo (Fiesta→Ford, Gol→Volkswagen, Corsa→Chevrolet, Cronos→Fiat, Hilux→Toyota), completala. Si no podés, "".
- modelo: solo el modelo (ej "Gol Trend", "Hilux"). version: motor/equipamiento/trim (ej "1.6 SE Plus", "SRV 4x4 AT").
- anio: 4 dígitos. km: solo dígitos ("120 mil" → 120000; "80.000 km" → 80000). Si no está, "".
- precio: solo dígitos en la moneda indicada. "12.500.000", "$12,5", "12.5M", "12 palos" → 12500000 / 12000000. "USD 8.500", "u$s 8500", "8500 dólares" → 8500 con moneda "USD". Si dice "consultar", "a convenir" o no hay precio → "".
- telefono: número de contacto si el aviso lo trae (solo dígitos); si no, "".
- notas: datos útiles y breves (GNC, único dueño, service oficial, permuta, financiación, patente/dominio, dónde está el auto), máximo 200 caracteres.
- No inventes datos. Lo que no esté en el texto se deja vacío.`;

async function llamarClaude(texto) {
  const body = {
    model: MODEL, max_tokens: 6000, temperature: 0, system: SYSTEM_EXTRACTOR,
    messages: [{ role: 'user', content: String(texto).slice(0, 8000) }]
  };
  for (let intento = 0; intento < 2; intento++) {
    const r = await fetch(ANTHROPIC_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-api-key': process.env.ANTHROPIC_API_KEY || '', 'anthropic-version': '2023-06-01' },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(60000)
    });
    const data = await r.json().catch(() => ({}));
    if (r.ok) return (data.content && data.content[0] && data.content[0].text) || '';
    const saturado = r.status === 529 || r.status === 429 || (data.error && data.error.type === 'overloaded_error');
    if (saturado && intento === 0) { console.log('[CLAUDE] Saturado, reintento en 2.5s'); await sleep(2500); continue; }
    throw new Error((data.error && data.error.message) || ('HTTP ' + r.status));
  }
  throw new Error('Claude no respondió');
}

function parsearJsonArray(raw) {
  let t = String(raw || '').trim().replace(/^```(?:json)?\s*/i, '').replace(/```\s*$/, '').trim();
  const ini = t.indexOf('[');
  if (ini === -1) return [];
  t = t.slice(ini);
  const intentos = [t];
  const finArr = t.lastIndexOf(']');
  if (finArr > 0) intentos.push(t.slice(0, finArr + 1));
  const finObj = t.lastIndexOf('}');
  if (finObj > 0) intentos.push(t.slice(0, finObj + 1) + ']'); // respuesta cortada a la mitad
  for (const s of intentos) {
    try { const a = JSON.parse(s); if (Array.isArray(a)) return a; } catch (e) {}
  }
  return [];
}

function normalizarAuto(a) {
  if (!a || typeof a !== 'object') return null;
  const modelo = String(a.modelo || '').trim();
  if (!modelo) return null;
  const marca = String(a.marca || '').trim() || 'Sin especificar';
  let anio = String(a.anio || '').replace(/\D/g, '').slice(0, 4);
  if (anio && (Number(anio) < 1970 || Number(anio) > new Date().getFullYear() + 1)) anio = '';
  const kmNum = Math.min(Number(String(a.km || '').replace(/\D/g, '')) || 0, 999999);
  const moneda = /usd|u\$s|us\$|d[oó]lar/i.test(String(a.moneda || '')) ? 'USD' : 'ARS';
  let precio = String(a.precio || '').replace(/\D/g, '');
  let notas = String(a.notas || '').trim().slice(0, 200);
  // Precios que no tienen sentido para un auto → se descartan en vez de mostrar algo incorrecto
  if (precio && ((moneda === 'ARS' && Number(precio) < 1000000) || (moneda === 'USD' && (Number(precio) < 1000 || Number(precio) > 300000)))) {
    notas = (notas ? notas + ' | ' : '') + 'Precio dudoso en el aviso: ' + precio;
    precio = '';
  }
  if (!anio && !precio) return null; // aviso demasiado vago para servir de algo
  return {
    marca, modelo, version: String(a.version || '').trim(), anio, km: kmNum,
    color: String(a.color || '').trim(), precio, moneda, notas,
    telefono: String(a.telefono || '').replace(/\D/g, '').replace(/^54/, '')
  };
}

async function extraerAutos(texto) {
  const raw = await llamarClaude(texto);
  return parsearJsonArray(raw).map(normalizarAuto).filter(Boolean);
}

// ── Ruthina ──────────────────────────────────────────────────────────────────
async function enviarARuthina(auto) {
  const headers = { 'Content-Type': 'application/json' };
  if (IMPORT_KEY) headers['x-import-key'] = IMPORT_KEY;
  const r = await fetch(RUTHINA_URL + '/api/stock/import', {
    method: 'POST', headers, body: JSON.stringify(auto), signal: AbortSignal.timeout(20000)
  });
  const data = await r.json().catch(() => ({}));
  if (!r.ok || !data.ok) throw new Error(data.error || ('HTTP ' + r.status));
  return data.accion;
}

// ── Grupos ───────────────────────────────────────────────────────────────────
async function nombreDeGrupo(jid) {
  const row = db.prepare('SELECT nombre FROM grupos WHERE jid = ?').get(jid);
  if (row && row.nombre) return row.nombre;
  if (EVO_URL && EVO_APIKEY && EVO_INSTANCE) {
    try {
      const r = await fetch(EVO_URL + '/group/findGroupInfos/' + encodeURIComponent(EVO_INSTANCE) + '?groupJid=' + encodeURIComponent(jid),
        { headers: { apikey: EVO_APIKEY }, signal: AbortSignal.timeout(10000) });
      const d = await r.json().catch(() => ({}));
      const nombre = d.subject || d.name || '';
      if (nombre) { db.prepare('UPDATE grupos SET nombre = ? WHERE jid = ?').run(nombre, jid); return nombre; }
    } catch (e) { console.log('[GRUPO] No pude traer el nombre de', jid, '-', e.message); }
  }
  return jid;
}

function registrarGrupo(jid) {
  const ahora = Date.now();
  let g = db.prepare('SELECT * FROM grupos WHERE jid = ?').get(jid);
  if (!g) {
    db.prepare('INSERT INTO grupos (jid, activo, primer_visto, ultimo_msg, msgs_total) VALUES (?,0,?,?,1)').run(jid, ahora, ahora);
    console.log('[GRUPO] Grupo nuevo detectado: ' + jid + ' (queda inactivo hasta que lo actives en /admin)');
    nombreDeGrupo(jid).catch(() => {});
    g = db.prepare('SELECT * FROM grupos WHERE jid = ?').get(jid);
  } else {
    db.prepare('UPDATE grupos SET ultimo_msg = ?, msgs_total = msgs_total + 1 WHERE jid = ?').run(ahora, jid);
  }
  return g;
}

// Quién publicó el aviso. En grupos, WhatsApp a veces oculta el número real (identificadores
// "@lid"); en ese caso queda solo el nombre.
function datosRemitente(msg) {
  const key = msg.key || {};
  const cands = [key.participantAlt, key.participant, msg.participant].filter(Boolean);
  const jidTel = cands.find(j => /@s\.whatsapp\.net$/.test(j));
  const telefono = jidTel ? jidTel.replace(/@.*/, '').replace(/\D/g, '').replace(/^54/, '') : '';
  return { nombre: (msg.pushName || '').trim(), telefono, jidRaw: cands[0] || '' };
}

// ── Cola: procesa de a un aviso (cuida el gasto de Claude y no satura a Ruthina) ─
let cola = Promise.resolve();
let pendientes = 0;
function encolar(fn) {
  pendientes++;
  cola = cola.then(fn).catch(e => console.error('[COLA]', e.message)).finally(() => { pendientes--; });
}

async function procesarAviso({ jid, texto, remitente }) {
  const grupoNombre = await nombreDeGrupo(jid);
  const claveTxt = 'txt:' + sha1((remitente.telefono || remitente.jidRaw || remitente.nombre) + '|' + normalizarTexto(texto));
  if (yaVisto(claveTxt)) { console.log('[GRUPO] Aviso repetido de ' + (remitente.nombre || 'alguien') + ', se omite'); return; }

  let autos;
  try { autos = await extraerAutos(texto); }
  catch (e) { console.error('[CLAUDE] Error:', e.message); return; } // sin marcar como visto: si lo reenvían, reintenta
  marcarVisto(claveTxt);
  if (!autos.length) return;

  console.log('[GRUPO] ' + grupoNombre + ' | ' + (remitente.nombre || 'sin nombre') + ' | ' + (remitente.telefono || 'sin-tel') + ' → ' + autos.length + ' auto(s)');
  for (const a of autos) {
    const telUbic = remitente.telefono || a.telefono;
    const quien = [remitente.nombre, telUbic].filter(Boolean).join(' - ');
    const payload = {
      marca: a.marca, modelo: a.modelo, version: a.version, anio: a.anio, km: a.km, color: a.color,
      precio: a.precio, moneda: a.moneda, estado: 'Disponible',
      notas: [a.notas, 'Grupo: ' + grupoNombre].filter(Boolean).join(' | '),
      ubicacion: quien || ('Grupo ' + grupoNombre),
      telefono: a.telefono || remitente.telefono || '',
      origen: 'grupo'
    };
    const base = { grupo: grupoNombre, vendedor: remitente.nombre, telefono: payload.telefono, marca: a.marca, modelo: a.modelo, anio: a.anio, precio: a.precio };
    try {
      const accion = await enviarARuthina(payload);
      db.prepare('UPDATE grupos SET autos_cargados = autos_cargados + 1 WHERE jid = ?').run(jid);
      logAuto({ ...base, accion });
      console.log('[STOCK] ' + accion + ': ' + a.marca + ' ' + a.modelo + ' ' + a.anio + ' (' + payload.ubicacion + ')');
    } catch (e) {
      logAuto({ ...base, accion: 'error', error: e.message });
      console.error('[STOCK] Error enviando a Ruthina:', e.message);
    }
  }
}

function recibirMensaje(msg) {
  const key = msg && msg.key;
  if (!key || key.fromMe) return;
  const jid = key.remoteJid || '';
  const esGrupo = jid.endsWith('@g.us');
  console.log('[WEBHOOK] Mensaje recibido de ' + jid + (esGrupo ? ' (grupo)' : ' (privado, se ignora)'));
  if (!esGrupo) return;               // solo grupos
  if (key.id) { if (yaVisto('id:' + key.id)) return; marcarVisto('id:' + key.id); }

  const m = msg.message || {};
  const texto = String(m.conversation || (m.extendedTextMessage && m.extendedTextMessage.text) ||
    (m.imageMessage && m.imageMessage.caption) || (m.videoMessage && m.videoMessage.caption) ||
    (m.documentMessage && m.documentMessage.caption) || '').trim();

  const grupo = registrarGrupo(jid);
  if (!grupo.activo) return;
  if (!pareceAviso(texto)) return;
  if (pendientes >= MAX_COLA) { console.warn('[COLA] Llena (' + pendientes + '), se descarta un mensaje'); return; }
  const remitente = datosRemitente(msg);
  encolar(() => procesarAviso({ jid, texto, remitente }));
}

// ── Servidor ─────────────────────────────────────────────────────────────────
const app = express();
app.use(cors({ origin: '*' }));
app.use(express.json({ limit: '30mb' })); // los webhooks con fotos pueden pesar

app.post('/webhook/evolution', (req, res) => {
  res.sendStatus(200); // responder rápido; el trabajo se hace después
  try {
    const body = req.body || {};
    const ev = String(body.event || '').toLowerCase().replace(/_/g, '.');
    console.log('[WEBHOOK] Evento recibido: ' + (ev || '(sin nombre de evento)'));
    if (ev !== 'messages.upsert') return;
    const d = body.data;
    const items = Array.isArray(d) ? d : (d && Array.isArray(d.messages) ? d.messages : (d ? [d] : []));
    for (const msg of items) recibirMensaje(msg);
  } catch (e) { console.error('[WEBHOOK]', e.message); }
});

app.get('/health', (_, res) => {
  const activos = db.prepare('SELECT COUNT(*) c FROM grupos WHERE activo = 1').get().c;
  res.json({ status: 'ok', modelo: MODEL, gruposActivos: activos, pendientes });
});

// ── Panel de administración ──────────────────────────────────────────────────
function auth(req, res, next) {
  if (!ADMIN_SECRET) return res.status(503).send('Falta definir ADMIN_SECRET en las variables de entorno.');
  const k = req.headers['x-admin-key'] || req.query.key;
  if (k !== ADMIN_SECRET) return res.status(401).send('No autorizado');
  next();
}

app.get('/admin/api/grupos', auth, (req, res) => {
  res.json(db.prepare('SELECT * FROM grupos ORDER BY activo DESC, ultimo_msg DESC').all());
});
app.post('/admin/api/grupos/toggle', auth, async (req, res) => {
  const { jid, activo } = req.body || {};
  if (!jid) return res.status(400).json({ error: 'Falta jid' });
  db.prepare('UPDATE grupos SET activo = ? WHERE jid = ?').run(activo ? 1 : 0, jid);
  if (activo) nombreDeGrupo(jid).catch(() => {});
  res.json({ ok: true });
});
app.get('/admin/api/log', auth, (req, res) => {
  res.json(db.prepare('SELECT * FROM log_autos ORDER BY id DESC LIMIT 40').all());
});
app.post('/admin/api/probar', auth, async (req, res) => {
  const texto = String((req.body || {}).texto || '');
  try {
    const autos = pareceAviso(texto) ? await extraerAutos(texto) : [];
    res.json({ pareceAviso: pareceAviso(texto), autos });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

const ADMIN_HTML = `<!doctype html><html lang="es"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Tutu Grupos</title>
<style>
body{font-family:system-ui,sans-serif;background:#0f1220;color:#eee;margin:0 auto;padding:16px;max-width:860px}
h1{font-size:20px} h2{font-size:15px;margin-top:28px;color:#9ab}
.card{background:#1a1f36;border-radius:10px;padding:10px 14px;margin:8px 0;display:flex;justify-content:space-between;align-items:center;gap:10px}
.sub{font-size:12px;color:#8a93b0;margin-top:2px;word-break:break-all}
button{border:0;border-radius:8px;padding:7px 12px;font-weight:700;cursor:pointer;color:#fff;background:#c8102e}
button.on{background:#16a34a}
textarea{width:100%;box-sizing:border-box;background:#0d1020;color:#eee;border:1px solid #333;border-radius:8px;padding:8px}
pre{background:#0d1020;border-radius:8px;padding:10px;white-space:pre-wrap;font-size:12px}
.err{color:#f87171}
</style></head><body>
<h1>Bot de grupos</h1>
<h2>Grupos detectados (activá los que quieras que lea)</h2><div id="grupos">Cargando…</div>
<h2>Probar un aviso (no carga nada en Ruthina)</h2>
<textarea id="t" rows="5" placeholder="Pegá acá el texto de un aviso de un grupo"></textarea>
<p><button id="probar" class="on">Probar</button></p><pre id="out"></pre>
<h2>Últimos autos procesados</h2><div id="log">Cargando…</div>
<script>
var KEY = new URLSearchParams(location.search).get('key') || '';
function api(path, opts){
  opts = opts || {};
  opts.headers = Object.assign({'x-admin-key': KEY, 'Content-Type': 'application/json'}, opts.headers || {});
  return fetch(path, opts).then(function(r){ return r.json(); });
}
function el(tag, txt, cls){ var e = document.createElement(tag); if(txt!==undefined) e.textContent = txt; if(cls) e.className = cls; return e; }
function fecha(ts){ return ts ? new Date(ts).toLocaleString('es-AR') : '-'; }
function cargarGrupos(){
  api('/admin/api/grupos').then(function(gs){
    var box = document.getElementById('grupos'); box.textContent = '';
    if(!gs.length){ box.textContent = 'Todavía no llegó ningún mensaje de grupos. Escribí algo en un grupo donde esté el número del bot.'; return; }
    gs.forEach(function(g){
      var card = el('div', undefined, 'card'), left = el('div');
      left.appendChild(el('div', g.nombre || g.jid));
      left.appendChild(el('div', 'mensajes: ' + g.msgs_total + ' · autos cargados: ' + g.autos_cargados + ' · último: ' + fecha(g.ultimo_msg), 'sub'));
      var b = el('button', g.activo ? 'Activo ✓' : 'Inactivo', g.activo ? 'on' : '');
      b.onclick = function(){ api('/admin/api/grupos/toggle', {method:'POST', body: JSON.stringify({jid: g.jid, activo: g.activo ? 0 : 1})}).then(cargarGrupos); };
      card.appendChild(left); card.appendChild(b); box.appendChild(card);
    });
  });
}
function cargarLog(){
  api('/admin/api/log').then(function(rows){
    var box = document.getElementById('log'); box.textContent = '';
    if(!rows.length){ box.textContent = 'Nada todavía.'; return; }
    rows.forEach(function(r){
      var card = el('div', undefined, 'card'), left = el('div');
      left.appendChild(el('div', (r.marca || '') + ' ' + (r.modelo || '') + ' ' + (r.anio || '') + (r.precio ? ' · $' + r.precio : '')));
      left.appendChild(el('div', fecha(r.ts) + ' · ' + (r.vendedor || 'sin nombre') + ' ' + (r.telefono || '') + ' · ' + (r.grupo || ''), 'sub'));
      if(r.error) left.appendChild(el('div', r.error, 'sub err'));
      card.appendChild(left); card.appendChild(el('div', r.accion, 'sub')); box.appendChild(card);
    });
  });
}
document.getElementById('probar').onclick = function(){
  var out = document.getElementById('out'); out.textContent = 'Procesando…';
  api('/admin/api/probar', {method:'POST', body: JSON.stringify({texto: document.getElementById('t').value})})
    .then(function(d){ out.textContent = JSON.stringify(d, null, 2); });
};
cargarGrupos(); cargarLog(); setInterval(function(){ cargarGrupos(); cargarLog(); }, 15000);
</script></body></html>`;
app.get('/admin', auth, (req, res) => res.type('html').send(ADMIN_HTML));

app.listen(PORT, () => {
  console.log('Tutu Grupos escuchando en :' + PORT);
  console.log('Modelo:', MODEL, '| Ruthina:', RUTHINA_URL, '| DB:', DB_PATH);
  console.log('ANTHROPIC_API_KEY:', process.env.ANTHROPIC_API_KEY ? 'ok' : 'FALTA',
    '| ADMIN_SECRET:', ADMIN_SECRET ? 'ok' : 'FALTA',
    '| Evolution (nombres de grupos):', (EVO_URL && EVO_APIKEY && EVO_INSTANCE) ? 'ok' : 'sin configurar',
    '| IMPORT_KEY:', IMPORT_KEY ? 'ok' : 'no usada');
});
