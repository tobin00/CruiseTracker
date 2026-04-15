'use strict';
// ═══════════════════════════════════════════════════════════════
//  Disney Cruise Tracker — app.js
//  MapLibre GL JS frontend: Real-time / Time-lapse / Follow modes
// ═══════════════════════════════════════════════════════════════

// ── Constants ──────────────────────────────────────────────────
const DATA_BASE   = './data/';
const MAP_STYLE   = 'https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json';
const TRAIL_HOURS = 24;          // hours of trail to show
const FOLLOW_DWELL_MS  = 18000; // ms per ship in follow mode
const REALTIME_REFRESH_MS = 30000;
const STEP_MS     = 10 * 60 * 1000; // 10 min in ms (matches build.py cadence)

// One distinct colour per ship (Disney-adjacent, works on dark map)
const SHIP_COLORS = {
  'Adventure': '#ff6b35',
  'Destiny':   '#e040fb',
  'Dream':     '#00b4d8',
  'Fantasy':   '#f72585',
  'Magic':     '#4cc9f0',
  'Wish':      '#ffd166',
  'Wonder':    '#06d6a0',
};
const DEFAULT_COLOR = '#aaaaaa';

// Speed levels for time-lapse (ms of data per second of real time)
const TL_SPEEDS = [
  { label: '1 hr/s',  ms: 60*60*1000 },
  { label: '6 hr/s',  ms: 6*60*60*1000 },
  { label: '1 dy/s',  ms: 24*60*60*1000 },
  { label: '1 wk/s',  ms: 7*24*60*60*1000 },
  { label: '1 mo/s',  ms: 30*24*60*60*1000 },
];
const TL_DEFAULT_SPEED_IDX = 3; // 1 wk/s

// ── State ───────────────────────────────────────────────────────
let map;
let shipData   = {};   // { shipName: [{t, lat, lon, s, p}, ...] }
let manifest   = null;
let currentMode = 'realtime';

// time-lapse
let tlPlaying  = false;
let tlSpeedIdx = TL_DEFAULT_SPEED_IDX;
let tlCurrentMs = 0;    // current position in ms since epoch
let tlMinMs    = 0;
let tlMaxMs    = 0;
let tlRafId    = null;
let tlLastReal = null;

// follow mode
let followShips    = [];
let followIdx      = 0;
let followTimerId  = null;
let followPopup    = null;

// realtime
let rtTimerId  = null;

// ── Utilities ───────────────────────────────────────────────────
const $ = id => document.getElementById(id);
const clamp = (v, lo, hi) => Math.min(hi, Math.max(lo, v));

function isoToMs(iso) { return new Date(iso).getTime(); }
function msToIso(ms)  { return new Date(ms).toISOString(); }

function fmtDate(ms) {
  const d = new Date(ms);
  return d.toUTCString().replace(' GMT','Z').replace(':00Z','Z');
}

function fmtDateShort(ms) {
  return new Date(ms).toLocaleDateString('en-US',
    { month:'short', day:'numeric', year:'numeric', timeZone:'UTC' });
}

/** Binary search: index of last record with t <= targetMs */
function bisectRight(records, targetMs) {
  let lo = 0, hi = records.length - 1, pos = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (isoToMs(records[mid].t) <= targetMs) { pos = mid; lo = mid + 1; }
    else hi = mid - 1;
  }
  return pos;
}

/** Interpolate lat/lon between two records */
function interpolate(r0, r1, targetMs) {
  const t0 = isoToMs(r0.t), t1 = isoToMs(r1.t);
  if (t1 === t0) return { lat: r0.lat, lon: r0.lon };
  const f = clamp((targetMs - t0) / (t1 - t0), 0, 1);
  // Handle antimeridian: choose shortest lon path
  let dlon = r1.lon - r0.lon;
  if (dlon > 180) dlon -= 360;
  if (dlon < -180) dlon += 360;
  return { lat: r0.lat + f*(r1.lat - r0.lat), lon: r0.lon + f*dlon };
}

/** Get ship position (and context) at a given timestamp (ms) */
function shipPositionAt(name, targetMs) {
  const recs = shipData[name];
  if (!recs || !recs.length) return null;
  const idx = bisectRight(recs, targetMs);
  const r0  = recs[idx];
  const r1  = recs[Math.min(idx+1, recs.length-1)];
  const { lat, lon } = interpolate(r0, r1, targetMs);
  return { lat, lon, status: r0.s, port: r0.p, nextPort: r1.p, record: r0 };
}

/** Get trail points for last N hours */
function trailPoints(name, targetMs) {
  const recs = shipData[name];
  if (!recs || !recs.length) return [];
  const startMs = targetMs - TRAIL_HOURS * 60 * 60 * 1000;
  const endIdx  = bisectRight(recs, targetMs);
  const out = [];
  for (let i = 0; i <= endIdx; i++) {
    const ms = isoToMs(recs[i].t);
    if (ms >= startMs) out.push([recs[i].lon, recs[i].lat]);
  }
  return out;
}

// ── Data loading ────────────────────────────────────────────────
async function loadAllData() {
  setLoadingBar(5);
  const res = await fetch(DATA_BASE + 'manifest.json');
  if (!res.ok) throw new Error('manifest.json not found');
  manifest = await res.json();

  const ships = manifest.ships;
  let loaded = 0;
  setLoadingBar(10);

  await Promise.all(ships.map(async (ship) => {
    const slug = ship.toLowerCase();
    const r = await fetch(DATA_BASE + slug + '.json.gz');
    if (!r.ok) { console.warn(`Missing data for ${ship}`); loaded++; return; }

    // Decompress gzip in browser using DecompressionStream
    const ds = new DecompressionStream('gzip');
    const piped = r.body.pipeThrough(ds);
    const reader = piped.getReader();
    const chunks = [];
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      chunks.push(value);
    }
    const text = new TextDecoder().decode(
      chunks.reduce((a, b) => { const c = new Uint8Array(a.length+b.length); c.set(a); c.set(b,a.length); return c; }, new Uint8Array())
    );
    shipData[ship] = JSON.parse(text);
    loaded++;
    setLoadingBar(10 + Math.round(85 * loaded / ships.length));
  }));

  // Compute global time range
  tlMinMs = manifest.date_range?.from ? isoToMs(manifest.date_range.from + 'T00:00:00Z') : 0;
  tlMaxMs = manifest.date_range?.to   ? isoToMs(manifest.date_range.to   + 'T23:59:00Z') : Date.now();
  tlCurrentMs = clamp(Date.now(), tlMinMs, tlMaxMs);

  setLoadingBar(100);
  followShips = Object.keys(shipData).filter(s => shipData[s]?.length > 0);
}

function setLoadingBar(pct) {
  $('loading-bar').style.width = pct + '%';
}

// ── Map initialisation ──────────────────────────────────────────
function initMap() {
  map = new maplibregl.Map({
    container: 'map',
    style: MAP_STYLE,
    center: [-40, 25],
    zoom: 2.2,
    minZoom: 1.5,
    maxZoom: 14,
    attributionControl: true,
  });

  map.addControl(new maplibregl.NavigationControl({ showCompass: false }), 'bottom-right');
  return new Promise(resolve => map.on('load', resolve));
}

// ── Ship layers ─────────────────────────────────────────────────
function buildShipGeoJSON(targetMs) {
  const features = [];
  for (const name of followShips) {
    const pos = shipPositionAt(name, targetMs);
    if (!pos) continue;
    features.push({
      type: 'Feature',
      geometry: { type: 'Point', coordinates: [pos.lon, pos.lat] },
      properties: {
        name,
        color:  SHIP_COLORS[name] || DEFAULT_COLOR,
        status: pos.status,
        port:   pos.port || pos.nextPort || '',
      },
    });
  }
  return { type: 'FeatureCollection', features };
}

function buildTrailGeoJSON(targetMs) {
  const features = [];
  for (const name of followShips) {
    const pts = trailPoints(name, targetMs);
    if (pts.length < 2) continue;
    // Split at antimeridian
    const segments = splitAntimeridian(pts);
    for (const seg of segments) {
      if (seg.length >= 2) {
        features.push({
          type: 'Feature',
          geometry: { type: 'LineString', coordinates: seg },
          properties: { name, color: SHIP_COLORS[name] || DEFAULT_COLOR },
        });
      }
    }
  }
  return { type: 'FeatureCollection', features };
}

function splitAntimeridian(pts) {
  if (pts.length < 2) return [pts];
  const segs = [];
  let cur = [pts[0]];
  for (let i = 1; i < pts.length; i++) {
    const [lonA] = pts[i-1], [lonB] = pts[i];
    let dlon = lonB - lonA;
    if (dlon > 180) dlon -= 360;
    if (dlon < -180) dlon += 360;
    if (Math.abs(lonB - lonA) > 180 && Math.abs(dlon) < Math.abs(lonB - lonA)) {
      // crosses antimeridian
      segs.push(cur);
      cur = [pts[i]];
    } else {
      cur.push(pts[i]);
    }
  }
  segs.push(cur);
  return segs;
}

function addShipLayers() {
  // Ship icon: create a simple coloured circle SVG registered as a map image
  // We use a circle marker drawn via a symbol layer with dynamic colour from data
  // (MapLibre supports data-driven paint on circle layers)

  // Trail layer
  map.addSource('trails', { type: 'geojson', data: { type:'FeatureCollection', features:[] } });
  map.addLayer({
    id: 'trails',
    type: 'line',
    source: 'trails',
    paint: {
      'line-color':   ['get', 'color'],
      'line-width':   2.5,
      'line-opacity': 0.55,
      'line-blur':    0.5,
    },
    layout: { 'line-join': 'round', 'line-cap': 'round' },
  });

  // Glow layer (wider, more transparent)
  map.addLayer({
    id: 'trails-glow',
    type: 'line',
    source: 'trails',
    paint: {
      'line-color':   ['get', 'color'],
      'line-width':   8,
      'line-opacity': 0.12,
      'line-blur':    4,
    },
    layout: { 'line-join': 'round', 'line-cap': 'round' },
  }, 'trails');

  // Ship dots
  map.addSource('ships', { type: 'geojson', data: { type:'FeatureCollection', features:[] } });
  map.addLayer({
    id: 'ships-halo',
    type: 'circle',
    source: 'ships',
    paint: {
      'circle-radius':       14,
      'circle-color':        ['get', 'color'],
      'circle-opacity':      0.15,
      'circle-blur':         0.6,
      'circle-stroke-width': 0,
    },
  });
  map.addLayer({
    id: 'ships',
    type: 'circle',
    source: 'ships',
    paint: {
      'circle-radius':       6,
      'circle-color':        ['get', 'color'],
      'circle-stroke-color': '#ffffff',
      'circle-stroke-width': 1.5,
    },
  });

  // Ship name labels
  map.addLayer({
    id: 'ship-labels',
    type: 'symbol',
    source: 'ships',
    layout: {
      'text-field':          ['get', 'name'],
      'text-font':           ['Open Sans Bold', 'Arial Unicode MS Bold'],
      'text-size':           12,
      'text-offset':         [0, 1.4],
      'text-anchor':         'top',
      'text-allow-overlap':  false,
      'text-ignore-placement': false,
    },
    paint: {
      'text-color':       ['get', 'color'],
      'text-halo-color':  'rgba(5,17,40,0.9)',
      'text-halo-width':  2,
    },
  });

  // Click → show popup
  map.on('click', 'ships', e => {
    const f = e.features[0];
    const props = f.properties;
    const status = props.status === 'sea' ? '⛵ At sea'
                 : props.status === 'port' ? `⚓ In port: ${props.port}`
                 : props.status === 'depart' ? `🚢 Departing ${props.port}`
                 : props.status === 'arrive' ? `🏁 Arriving ${props.port}`
                 : props.status;
    new maplibregl.Popup({ closeButton: false, closeOnClick: true })
      .setLngLat(f.geometry.coordinates)
      .setHTML(`<strong style="color:#c8e6ff">${props.name}</strong><br>${status}`)
      .addTo(map);
  });
  map.on('mouseenter', 'ships', () => { map.getCanvas().style.cursor = 'pointer'; });
  map.on('mouseleave', 'ships', () => { map.getCanvas().style.cursor = ''; });
}

function updateMapData(targetMs) {
  const ships  = buildShipGeoJSON(targetMs);
  const trails = buildTrailGeoJSON(targetMs);
  map.getSource('ships')?.setData(ships);
  map.getSource('trails')?.setData(trails);
}

// ── Real-time mode ──────────────────────────────────────────────
let rtRafId = null;
let rtLastMapUpdate = 0;
const RT_MAP_INTERVAL_MS = 5000; // update ship positions every 5 seconds

function startRealtime() {
  stopAllModes();
  $('tl-controls').classList.add('hidden');
  $('ship-card').classList.add('hidden');
  rtLastMapUpdate = 0;
  rtRafId = requestAnimationFrame(rtTick);
}

function rtTick(ts) {
  const now = Date.now();
  const clamped = clamp(now, tlMinMs, tlMaxMs);

  // Clock ticks every frame — always live
  $('hud-clock').textContent = fmtDate(now) + ' UTC';

  // Ship positions update every 5 seconds (interpolation makes movement smooth)
  if (now - rtLastMapUpdate >= RT_MAP_INTERVAL_MS) {
    updateMapData(clamped);
    rtLastMapUpdate = now;
  }

  rtRafId = requestAnimationFrame(rtTick);
}

function stopRealtime() {
  if (rtRafId) { cancelAnimationFrame(rtRafId); rtRafId = null; }
}

// ── Time-lapse mode ─────────────────────────────────────────────
function startTimelapse() {
  stopAllModes();
  $('tl-controls').classList.remove('hidden');
  $('ship-card').classList.add('hidden');
  updateTlSlider();
  updateTlDate();
  updateMapData(tlCurrentMs);
}

function updateTlSlider() {
  const slider = $('tl-slider');
  const range  = tlMaxMs - tlMinMs;
  slider.value = range > 0 ? Math.round(1000 * (tlCurrentMs - tlMinMs) / range) : 0;
}

function updateTlDate() {
  $('tl-date').textContent = fmtDateShort(tlCurrentMs);
  $('hud-clock').textContent = fmtDate(tlCurrentMs) + ' UTC';
}

function tlTick(realNow) {
  if (!tlPlaying) return;
  if (tlLastReal === null) { tlLastReal = realNow; tlRafId = requestAnimationFrame(tlTick); return; }

  const realDelta = realNow - tlLastReal;
  tlLastReal = realNow;
  const speed = TL_SPEEDS[tlSpeedIdx].ms;
  tlCurrentMs += realDelta * (speed / 1000);

  if (tlCurrentMs > tlMaxMs) { tlCurrentMs = tlMinMs; } // loop

  updateMapData(tlCurrentMs);
  updateTlSlider();
  updateTlDate();
  tlRafId = requestAnimationFrame(tlTick);
}

function tlSetPlaying(val) {
  tlPlaying = val;
  $('tl-play').textContent = tlPlaying ? '⏸' : '▶';
  if (tlPlaying) { tlLastReal = null; tlRafId = requestAnimationFrame(tlTick); }
  else if (tlRafId) { cancelAnimationFrame(tlRafId); tlRafId = null; }
}

// ── Follow mode ─────────────────────────────────────────────────
function startFollow() {
  stopAllModes();
  $('tl-controls').classList.add('hidden');
  $('ship-card').classList.remove('hidden');
  followIdx = 0;
  flyToCurrentShip();
}

function flyToCurrentShip() {
  if (!followShips.length) return;
  const name = followShips[followIdx % followShips.length];
  const now  = Date.now();
  const pos  = shipPositionAt(name, clamp(now, tlMinMs, tlMaxMs));
  if (!pos) { advanceFollow(); return; }

  const color = SHIP_COLORS[name] || DEFAULT_COLOR;
  const statusText = pos.status === 'sea'
    ? `⛵ At sea${pos.nextPort ? ` → ${pos.nextPort}` : ''}`
    : pos.status === 'port' ? `⚓ In port`
    : pos.status === 'depart' ? `🚢 Departing`
    : pos.status === 'arrive' ? `🏁 Arriving`
    : pos.status;

  $('card-name').textContent   = `✦ Disney ${name}`;
  $('card-name').style.color   = color;
  $('card-status').textContent = statusText;
  $('card-dest').textContent   = (pos.port || pos.nextPort) ? `📍 ${pos.port || pos.nextPort}` : '';

  updateMapData(clamp(now, tlMinMs, tlMaxMs));
  $('hud-clock').textContent = fmtDate(now) + ' UTC';

  map.flyTo({
    center: [pos.lon, pos.lat],
    zoom: 5.5,
    duration: 3000,
    essential: true,
  });

  followTimerId = setTimeout(advanceFollow, FOLLOW_DWELL_MS);
}

function advanceFollow() {
  clearTimeout(followTimerId);
  followIdx = (followIdx + 1) % followShips.length;
  flyToCurrentShip();
}

// ── Mode switching ──────────────────────────────────────────────
function stopAllModes() {
  clearInterval(rtTimerId);    rtTimerId    = null;
  clearTimeout(followTimerId); followTimerId = null;
  if (tlRafId) { cancelAnimationFrame(tlRafId); tlRafId = null; }
  if (rtRafId) { cancelAnimationFrame(rtRafId); rtRafId = null; }
  tlPlaying = false;
  $('tl-play').textContent = '▶';
}

function switchMode(mode) {
  currentMode = mode;
  document.querySelectorAll('.mode-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.mode === mode);
  });
  // Reset to world view on mode switch (except follow, which flies itself)
  if (mode !== 'follow') {
    map.flyTo({ center: [-40, 25], zoom: 2.2, duration: 1500, essential: true });
  }
  if (mode === 'realtime')  startRealtime();
  if (mode === 'timelapse') startTimelapse();
  if (mode === 'follow')    startFollow();
}

// ── Controls wiring ─────────────────────────────────────────────
function wireControls() {
  // Mode buttons
  document.querySelectorAll('.mode-btn').forEach(btn => {
    btn.addEventListener('click', () => switchMode(btn.dataset.mode));
  });

  // Time-lapse play/pause
  $('tl-play').addEventListener('click', () => tlSetPlaying(!tlPlaying));

  // Time-lapse slider
  $('tl-slider').addEventListener('input', e => {
    const range = tlMaxMs - tlMinMs;
    tlCurrentMs = tlMinMs + (parseInt(e.target.value) / 1000) * range;
    updateMapData(tlCurrentMs);
    updateTlDate();
  });

  // Speed buttons
  $('tl-slower').addEventListener('click', () => {
    tlSpeedIdx = Math.max(0, tlSpeedIdx - 1);
    $('tl-speed-label').textContent = TL_SPEEDS[tlSpeedIdx].label;
  });
  $('tl-faster').addEventListener('click', () => {
    tlSpeedIdx = Math.min(TL_SPEEDS.length - 1, tlSpeedIdx + 1);
    $('tl-speed-label').textContent = TL_SPEEDS[tlSpeedIdx].label;
  });

  // Keyboard (also handles Fire Stick D-pad via keydown)
  document.addEventListener('keydown', e => {
    if (e.repeat) return;
    switch (e.key) {
      case '1': case 'r': case 'R': switchMode('realtime');  break;
      case '2': case 't': case 'T': switchMode('timelapse'); break;
      case '3': case 'f': case 'F': switchMode('follow');    break;

      case ' ':
        e.preventDefault();
        if (currentMode === 'timelapse') tlSetPlaying(!tlPlaying);
        if (currentMode === 'follow')    advanceFollow();
        break;
      case 'Enter':
        if (currentMode === 'timelapse') tlSetPlaying(!tlPlaying);
        if (currentMode === 'follow')    advanceFollow();
        break;

      case 'ArrowRight':
        if (currentMode === 'timelapse') {
          tlCurrentMs = clamp(tlCurrentMs + TL_SPEEDS[tlSpeedIdx].ms * 2, tlMinMs, tlMaxMs);
          updateMapData(tlCurrentMs); updateTlSlider(); updateTlDate();
        }
        break;
      case 'ArrowLeft':
        if (currentMode === 'timelapse') {
          tlCurrentMs = clamp(tlCurrentMs - TL_SPEEDS[tlSpeedIdx].ms * 2, tlMinMs, tlMaxMs);
          updateMapData(tlCurrentMs); updateTlSlider(); updateTlDate();
        }
        break;
      case 'ArrowUp':
        if (currentMode === 'timelapse') {
          tlSpeedIdx = Math.min(TL_SPEEDS.length - 1, tlSpeedIdx + 1);
          $('tl-speed-label').textContent = TL_SPEEDS[tlSpeedIdx].label;
        }
        break;
      case 'ArrowDown':
        if (currentMode === 'timelapse') {
          tlSpeedIdx = Math.max(0, tlSpeedIdx - 1);
          $('tl-speed-label').textContent = TL_SPEEDS[tlSpeedIdx].label;
        }
        break;

      case '+': case '=':
        map.zoomIn({ duration: 300 }); break;
      case '-':
        map.zoomOut({ duration: 300 }); break;
      case 'Escape':
        map.flyTo({ center: [-40, 25], zoom: 2.2, duration: 1000 }); break;
    }
  });

  // Fade out key hint after 6s
  setTimeout(() => $('key-hint').classList.add('hidden'), 6000);
}

// ── Boot ─────────────────────────────────────────────────────────
(async function init() {
  try {
    $('loading-sub').textContent = 'Initialising map…';
    const [,] = await Promise.all([loadAllData(), initMap()]);

    $('loading-sub').textContent = 'Setting up layers…';
    addShipLayers();
    wireControls();

    // Set initial speed label
    $('tl-speed-label').textContent = TL_SPEEDS[tlSpeedIdx].label;

    // Start in real-time mode
    switchMode('realtime');

    // Hide loading screen
    const loadEl = $('loading');
    loadEl.classList.add('fade-out');
    setTimeout(() => loadEl.remove(), 900);

  } catch (err) {
    console.error(err);
    $('loading-sub').textContent = '⚠ Failed to load data. Check console.';
    $('loading-bar').style.background = '#e74c3c';
  }
})();
