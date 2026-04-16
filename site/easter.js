'use strict';
// ═══════════════════════════════════════════════════════════════
//  Disney Easter Egg Engine — easter.js
//
//  To add a new egg:
//    1. Write a function   function eggMyThing() { ... }
//    2. Add it to EGGS:   myThing: eggMyThing
//    3. Add timing to EGG_CONFIG: myThing: { interval_ms: 4 * 60 * 1000 }
//
//  Eggs run on a random timer (±30% jitter). Frequency multiplier
//  scales all intervals: calm=3x longer, magical=0.35x shorter.
// ═══════════════════════════════════════════════════════════════

// ── Timing config — edit this to tune each egg ────────────────
const EGG_CONFIG = {
  tinkerbell:   { interval_ms:  3 * 60 * 1000 },  // ~every 3 min
  shootingStar: { interval_ms:  2 * 60 * 1000 },  // ~every 2 min
  flyingFish:   { interval_ms: 90 * 1000       },  // ~every 90 sec
  whale:        { interval_ms:  7 * 60 * 1000 },  // ~every 7 min
  mermaid:      { interval_ms:  5 * 60 * 1000 },  // ~every 5 min
  bottle:       { interval_ms: 12 * 60 * 1000 },  // ~every 12 min
};

const FREQ_MULT = { calm: 3.0, normal: 1.0, magical: 0.35 };

// ── State ─────────────────────────────────────────────────────
let eggEnabled   = true;
let eggFrequency = 'normal';
let eggTimers    = {};
let overlay      = null;

// ── Utilities ─────────────────────────────────────────────────
const rnd  = (lo, hi) => lo + Math.random() * (hi - lo);

function makeEl(tag, css, html) {
  const el = document.createElement(tag);
  Object.assign(el.style, css);
  if (html !== undefined) el.innerHTML = html;
  overlay.appendChild(el);
  return el;
}

function removeEl(el) {
  if (el && el.parentNode) el.parentNode.removeChild(el);
}

// ── Easter Eggs ───────────────────────────────────────────────

// Tinkerbell: glowing dot follows sine-wave path, drops sparkles
function eggTinkerbell() {
  const W = window.innerWidth, H = window.innerHeight;
  const startY   = rnd(H * 0.15, H * 0.65);
  const amplitude = rnd(40, 90);
  const waveFreq  = rnd(2, 4);
  const duration  = rnd(4500, 6500);
  const startX    = -20;
  const endX      = W + 20;

  const tink = makeEl('div', {
    position: 'absolute', pointerEvents: 'none', zIndex: '200',
    width: '12px', height: '12px', borderRadius: '50%',
    background: 'radial-gradient(circle, #fff 0%, #ffe066 45%, rgba(255,210,0,0) 100%)',
    boxShadow: '0 0 10px 5px rgba(255,220,50,0.75), 0 0 22px 8px rgba(255,200,0,0.35)',
  });

  const start = performance.now();
  let sparkTimer = null;

  function spawnSparkle(x, y) {
    for (let i = 0; i < 3; i++) {
      const size = rnd(3, 7);
      const sp = makeEl('div', {
        position: 'absolute', pointerEvents: 'none', zIndex: '199',
        width: size + 'px', height: size + 'px', borderRadius: '50%',
        background: `hsl(${Math.floor(rnd(40, 60))},100%,80%)`,
        boxShadow: '0 0 5px 2px rgba(255,220,60,0.55)',
        left: (x + rnd(-10, 10)) + 'px',
        top:  (y + rnd(-10, 10)) + 'px',
        transition: 'opacity 0.7s ease, transform 0.7s ease',
        opacity: '1',
      });
      requestAnimationFrame(() => {
        sp.style.opacity = '0';
        sp.style.transform = `translate(${rnd(-18,18)}px,${rnd(-28,-4)}px) scale(0.1)`;
      });
      setTimeout(() => removeEl(sp), 800);
    }
  }

  function tick() {
    const t = (performance.now() - start) / duration;
    if (t > 1) { removeEl(tink); return; }
    const x = startX + (endX - startX) * t;
    const y = startY + Math.sin(t * waveFreq * Math.PI * 2) * amplitude;
    tink.style.left = (x - 6) + 'px';
    tink.style.top  = (y - 6) + 'px';
    requestAnimationFrame(tick);
  }

  sparkTimer = setInterval(() => {
    const t = (performance.now() - start) / duration;
    const x = startX + (endX - startX) * t;
    const y = startY + Math.sin(t * waveFreq * Math.PI * 2) * amplitude;
    spawnSparkle(x, y);
  }, 110);

  requestAnimationFrame(tick);
  setTimeout(() => clearInterval(sparkTimer), duration + 200);
}

// Shooting star: bright streak diagonally across upper screen
function eggShootingStar() {
  const W = window.innerWidth;
  const startX   = rnd(W * 0.05, W * 0.45);
  const startY   = rnd(20, 100);
  const tailLen  = rnd(130, 230);
  const angle    = rnd(22, 42);
  const duration = rnd(800, 1300);

  const star = makeEl('div', {
    position: 'absolute', pointerEvents: 'none', zIndex: '200',
    left: startX + 'px', top: startY + 'px',
    width: tailLen + 'px', height: '2px',
    background: 'linear-gradient(90deg, rgba(255,255,255,0) 0%, rgba(255,255,230,0.85) 55%, #fff 100%)',
    borderRadius: '2px',
    transform: `rotate(${angle}deg)`,
    transformOrigin: 'right center',
    opacity: '0',
  });

  const travel = tailLen * 4;
  const keyframes = [
    { opacity: 0, transform: `rotate(${angle}deg) translateX(0)` },
    { opacity: 1, transform: `rotate(${angle}deg) translateX(${travel * 0.1}px)`, offset: 0.08 },
    { opacity: 0, transform: `rotate(${angle}deg) translateX(${travel}px)` },
  ];
  const anim = star.animate(keyframes, { duration, easing: 'ease-in', fill: 'forwards' });
  anim.onfinish = () => removeEl(star);
}

// Flying fish: arcs up near a random spot
function eggFlyingFish() {
  const W = window.innerWidth, H = window.innerHeight;
  const x      = rnd(W * 0.1, W * 0.88);
  const baseY  = rnd(H * 0.45, H * 0.78);
  const peakY  = baseY - rnd(90, 170);
  const dur    = rnd(1800, 2600);
  const flip   = Math.random() > 0.5 ? 'scaleX(-1)' : '';

  const fish = makeEl('div', {
    position: 'absolute', pointerEvents: 'none', zIndex: '200',
    fontSize: '28px', left: x + 'px', top: baseY + 'px',
    userSelect: 'none',
    filter: 'drop-shadow(0 2px 4px rgba(0,180,220,0.5))',
  }, '🐟');

  const keyframes = [
    { top: baseY + 'px',  opacity: 0, transform: `rotate(-35deg) ${flip}` },
    { top: peakY + 'px',  opacity: 1, transform: `rotate(0deg) ${flip}`,   offset: 0.42 },
    { top: baseY + 'px',  opacity: 0, transform: `rotate(35deg) ${flip}` },
  ];
  const anim = fish.animate(keyframes, { duration: dur, easing: 'ease-in-out', fill: 'forwards' });
  anim.onfinish = () => removeEl(fish);
}

// Whale breach: rises dramatically, splashes back
function eggWhale() {
  const W = window.innerWidth, H = window.innerHeight;
  const x     = rnd(W * 0.15, W * 0.82);
  const baseY = rnd(H * 0.42, H * 0.74);
  const dur   = 2800;

  const whale = makeEl('div', {
    position: 'absolute', pointerEvents: 'none', zIndex: '200',
    fontSize: '52px', left: x + 'px', top: (baseY + 70) + 'px',
    userSelect: 'none',
    filter: 'drop-shadow(0 6px 12px rgba(0,150,180,0.55))',
  }, '🐋');

  const keyframes = [
    { top: (baseY + 90) + 'px', opacity: 0, transform: 'rotate(0deg)' },
    { top: (baseY - 50) + 'px', opacity: 1, transform: 'rotate(-18deg)', offset: 0.38 },
    { top: (baseY - 30) + 'px', opacity: 0.8, transform: 'rotate(5deg)',  offset: 0.65 },
    { top: (baseY + 90) + 'px', opacity: 0, transform: 'rotate(22deg)' },
  ];
  const anim = whale.animate(keyframes, { duration: dur, easing: 'ease-in-out', fill: 'forwards' });
  anim.onfinish = () => removeEl(whale);
}

// Mermaid: swims slowly across the ocean
function eggMermaid() {
  const W = window.innerWidth, H = window.innerHeight;
  const y         = rnd(H * 0.38, H * 0.72);
  const fromRight = Math.random() > 0.5;
  const startX    = fromRight ? W + 60 : -60;
  const endX      = fromRight ? -60 : W + 60;
  const dur       = rnd(7000, 10000);
  const flipStyle = fromRight ? 'scaleX(1)' : 'scaleX(-1)';

  const mermaid = makeEl('div', {
    position: 'absolute', pointerEvents: 'none', zIndex: '200',
    fontSize: '42px', left: startX + 'px', top: y + 'px',
    userSelect: 'none',
    filter: 'drop-shadow(0 3px 8px rgba(6,214,160,0.65))',
  }, '🧜');

  const span = (endX - startX);
  const keyframes = [
    { left: startX + 'px',              opacity: 0, transform: flipStyle },
    { left: (startX + span*0.07)+'px',  opacity: 1, transform: flipStyle, offset: 0.07 },
    { left: (endX   - span*0.07)+'px',  opacity: 1, transform: flipStyle, offset: 0.93 },
    { left: endX + 'px',                opacity: 0, transform: flipStyle },
  ];
  const anim = mermaid.animate(keyframes, { duration: dur, easing: 'linear', fill: 'forwards' });
  anim.onfinish = () => removeEl(mermaid);
}

// Message in a bottle: drifts very slowly across the ocean
function eggBottle() {
  const W = window.innerWidth, H = window.innerHeight;
  const y         = rnd(H * 0.32, H * 0.74);
  const fromRight = Math.random() > 0.5;
  const startX    = fromRight ? W + 50 : -50;
  const endX      = fromRight ? -50 : W + 50;
  const dur       = rnd(15000, 22000);

  const bottle = makeEl('div', {
    position: 'absolute', pointerEvents: 'none', zIndex: '200',
    fontSize: '30px', left: startX + 'px', top: y + 'px',
    userSelect: 'none',
  }, '🍾');

  const span = (endX - startX);
  const keyframes = [
    { left: startX + 'px',             opacity: 0, transform: 'rotate(-12deg)' },
    { left: (startX + span*0.05)+'px', opacity: 0.8,transform: 'rotate(-8deg)',  offset: 0.05 },
    { left: (endX - span*0.05)+'px',   opacity: 0.8,transform: 'rotate(8deg)',   offset: 0.95 },
    { left: endX + 'px',               opacity: 0, transform: 'rotate(12deg)' },
  ];
  const anim = bottle.animate(keyframes, { duration: dur, easing: 'linear', fill: 'forwards' });
  anim.onfinish = () => removeEl(bottle);
}

// ── Egg registry ──────────────────────────────────────────────
// Add new eggs here — no other changes needed.
const EGGS = {
  tinkerbell:   eggTinkerbell,
  shootingStar: eggShootingStar,
  flyingFish:   eggFlyingFish,
  whale:        eggWhale,
  mermaid:      eggMermaid,
  bottle:       eggBottle,
};

// ── Scheduler ─────────────────────────────────────────────────
function scheduleEgg(name) {
  const cfg = EGG_CONFIG[name];
  if (!cfg) return;
  const mult   = FREQ_MULT[eggFrequency] || 1;
  const jitter = rnd(0.7, 1.3);
  const delay  = cfg.interval_ms * mult * jitter;

  eggTimers[name] = setTimeout(() => {
    if (eggEnabled && overlay) {
      try { EGGS[name](); } catch(e) { console.warn('Easter egg error:', name, e); }
    }
    scheduleEgg(name);
  }, delay);
}

// ── Public API ────────────────────────────────────────────────
function startEasterEggs() {
  overlay = document.getElementById('egg-overlay');
  if (!overlay) return;
  Object.keys(EGGS).forEach(name => scheduleEgg(name));
}

function stopAllEggs() {
  Object.values(eggTimers).forEach(clearTimeout);
  eggTimers = {};
}

function setEasterEnabled(val) {
  eggEnabled = !!val;
}

function setEasterFrequency(val) {
  eggFrequency = val;
  stopAllEggs();
  Object.keys(EGGS).forEach(name => scheduleEgg(name));
}
