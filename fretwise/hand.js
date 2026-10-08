/* Articulated fretting hand, drawn in SVG over the neck.
   View: high e string at the top (tab orientation), so the fretting fingers reach over the TREBLE (top) edge from a palm
   above the neck, and the thumb sits behind the neck. Each finger is drawn from its knuckle to its fingertip, which is
   placed just behind the fret wire (the most efficient place to press). Fingers that change position lift and arc to
   their new place over ~130 ms, finishing as the note sounds (the performance timeline shifts the hand 140 ms early).
   This is an illustration driven by real fret/string/finger data — not a biomechanically simulated hand. */
window.FRETWISE_HAND = (() => {
  const NS = 'http://www.w3.org/2000/svg';
  const TWEEN = 130;            // ms
  const FRETS = 5;
  const BEHIND_FRET = 0.76;     // fraction across the fret space where the fingertip lands (1 = on the wire)

  function create(neck) {
    const svg = document.createElementNS(NS, 'svg');
    svg.setAttribute('class', 'hand-svg'); svg.setAttribute('aria-hidden', 'true');
    svg.innerHTML = `<defs>
        <linearGradient id="skin" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#f0c09b"/><stop offset=".6" stop-color="#cf9370"/><stop offset="1" stop-color="#a86b4e"/></linearGradient>
        <radialGradient id="palmSkin" cx=".45" cy=".35" r=".8"><stop offset="0" stop-color="#e6b08a"/><stop offset=".7" stop-color="#b97b5a"/><stop offset="1" stop-color="#8d5a41"/></radialGradient>
      </defs>
      <g class="hand-shadow"></g><ellipse class="hand-palm-svg"/><g class="hand-fingers"></g>`;
    neck.append(svg);
    const palm = svg.querySelector('.hand-palm-svg');
    const fingerLayer = svg.querySelector('.hand-fingers');
    const shadowLayer = svg.querySelector('.hand-shadow');
    const fingers = {};
    for (let n = 1; n <= 4; n++) {
      const g = document.createElementNS(NS, 'g'); g.setAttribute('class', 'finger-g'); g.dataset.finger = String(n);
      g.innerHTML = `<path class="finger-outline"/><path class="finger-flesh"/><circle class="finger-pad"/><text class="finger-num">${n}</text>`;
      fingerLayer.append(g);
      const sh = document.createElementNS(NS, 'ellipse'); sh.setAttribute('class', 'finger-shadow'); shadowLayer.append(sh);
      fingers[n] = { g, outline: g.children[0], flesh: g.children[1], pad: g.children[2], num: g.children[3], shadow: sh, cur: null, from: null, to: null, start: 0 };
    }
    let pending = null, W = 0, H = 0, windowStart = 1, target = null, palmCur = null, palmFrom = null, palmTo = null, raf = null;

    const fretX = (fret, ws) => ((fret - ws + BEHIND_FRET) / FRETS) * W;
    const stringY = (s) => (((5 - s) + 0.5) / 6) * H;
    const knuckleY = () => -H * 0.30;
    // Knuckles sit roughly one fret apart: finger n hovers over fret (anchor + n - 1).
    const knuckleX = (n, anchor) => ((anchor + n - 1 - windowStart + 0.55) / FRETS) * W;

    function measure() { const r = neck.getBoundingClientRect(); const w = neck.clientWidth || r.width, h = neck.clientHeight || r.height; if (!w || !h) return false; W = w; H = h; svg.setAttribute('viewBox', `0 ${-H * 0.62} ${W} ${H * 1.62}`); svg.style.height = (H * 1.62) + 'px'; svg.style.top = (-H * 0.62) + 'px'; return true; }

    /* Target pose for a shape. Returns per-finger {x,y,placed,barre:{y1,y2}} and the hand anchor. */
    function pose(shape, ws) {
      const { frets, fingers: fing, barre } = shape;
      const placed = {};
      frets.forEach((f, s) => {
        const n = fing[s]; if (f < 1 || !n) return;
        if (barre && n === 1) return;
        if (!placed[n] || s < placed[n].s) placed[n] = { s, f, x: fretX(f, ws), y: stringY(s) };
      });
      if (barre) placed[1] = { s: barre.from, f: barre.fret, x: fretX(barre.fret, ws), y: (stringY(barre.to) + stringY(barre.from)) / 2, barre: { y1: stringY(barre.to) - 9, y2: stringY(barre.from) + 9 } };
      // anchor = fret under the first finger; derived from placed fingers so lifted fingers hover where they'd naturally sit
      let anchor = ws;
      const list = Object.entries(placed).map(([n, p]) => p.f - (Number(n) - 1));
      if (list.length) anchor = Math.round(list.reduce((a, b) => a + b, 0) / list.length);
      anchor = Math.max(ws, Math.min(ws + 1, anchor));
      const out = {};
      for (let n = 1; n <= 4; n++) {
        const kx = knuckleX(n, anchor);
        if (placed[n]) out[n] = { ...placed[n], kx, placed: true };
        else out[n] = { x: kx + W * 0.025, y: -H * 0.13 - (n === 4 ? H * 0.03 : 0), kx, placed: false };  // relaxed: curled just above the treble edge, clearly off the strings
      }
      return { fingers: out, palmX: (knuckleX(1, anchor) + knuckleX(4, anchor)) / 2 };
    }

    function setShape(shape, ws, immediate) {
      if (!measure()) { clearTimeout(pending); pending = setTimeout(() => setShape(shape, ws, true), 250); return; }  // neck hidden (another page is open)
      windowStart = ws;
      target = pose(shape, ws);
      const now = performance.now();
      for (let n = 1; n <= 4; n++) {
        const f = fingers[n]; const t = target.fingers[n];
        const moved = !f.cur || Math.abs(f.cur.x - t.x) > 1 || Math.abs(f.cur.y - t.y) > 1 || !!f.cur.barre !== !!t.barre || f.cur.placed !== t.placed;
        f.from = f.cur ? { ...f.cur } : { ...t }; f.to = t; f.start = immediate || !moved ? now - TWEEN : now; f.lift = moved && !immediate && f.cur && (f.cur.placed || t.placed);
        f.g.dataset.placed = t.placed ? '1' : '0'; f.g.dataset.string = t.placed && t.s !== undefined ? String(t.s) : ''; f.g.dataset.fret = t.placed ? String(t.f) : ''; f.g.classList.toggle('is-barre', !!t.barre);
      }
      palmFrom = palmCur === null ? target.palmX : palmCur; palmTo = target.palmX;
      if (immediate) { palmFrom = palmTo; }
      tick();
    }

    const ease = (k) => k < 0 ? 0 : k > 1 ? 1 : 1 - Math.pow(1 - k, 3);
    function tick() {
      cancelAnimationFrame(raf);
      const now = performance.now(); let busy = false;
      for (let n = 1; n <= 4; n++) {
        const f = fingers[n]; if (!f.to) continue;
        const k = ease((now - f.start) / TWEEN); if (k < 1) busy = true;
        const lift = f.lift ? Math.sin(Math.PI * Math.min(1, (now - f.start) / TWEEN)) * H * 0.22 : 0;
        const x = f.from.x + (f.to.x - f.from.x) * k, y = f.from.y + (f.to.y - f.from.y) * k - lift, kx = f.from.kx + (f.to.kx - f.from.kx) * k;
        f.cur = { ...f.to, x, y, kx }; if (k < 1) f.cur.placed = f.from.placed && f.to.placed ? true : f.to.placed;
        draw(n, f, x, y, kx, k < 1 ? lift : 0);
      }
      const kp = ease((now - (fingers[1].start || 0)) / TWEEN); palmCur = palmFrom + (palmTo - palmFrom) * kp;
      palm.setAttribute('cx', palmCur); palm.setAttribute('cy', knuckleY() - H * 0.2); palm.setAttribute('rx', W * 0.19); palm.setAttribute('ry', H * 0.26);
      if (busy) raf = requestAnimationFrame(tick);
    }

    function draw(n, f, x, y, kx, lifting) {
      const ky = knuckleY(); const t = f.to; const pressed = t.placed && !lifting;
      const width = n === 4 ? 13 : 15;
      let d;
      if (t.barre && !lifting) {
        // first finger laid flat across the strings: knuckle → top of barre, then straight down the fret
        d = `M ${kx} ${ky} Q ${x - 4} ${t.barre.y1 - 18} ${x} ${t.barre.y1} L ${x} ${t.barre.y2}`;
      } else {
        // two-segment finger with a bent middle joint (arched so it clears the strings above)
        const mx = (kx + x) / 2 - W * 0.012, my = Math.min(ky, y) + (Math.abs(y - ky)) * 0.42 - H * 0.12;
        d = `M ${kx} ${ky} Q ${mx} ${my} ${x} ${y}`;
      }
      f.outline.setAttribute('d', d); f.flesh.setAttribute('d', d);
      f.outline.setAttribute('stroke-width', width + 3); f.flesh.setAttribute('stroke-width', width);
      f.pad.setAttribute('cx', x); f.pad.setAttribute('cy', t.barre && !lifting ? t.barre.y1 + 8 : y); f.pad.setAttribute('r', width / 2 + 0.5);
      f.num.setAttribute('x', x); f.num.setAttribute('y', (t.barre && !lifting ? t.barre.y1 + 8 : y) + 3.5);
      f.g.classList.toggle('pressed', pressed); f.g.classList.toggle('relaxed', !t.placed);
      f.shadow.setAttribute('cx', x + 3); f.shadow.setAttribute('cy', (t.barre && !lifting ? (t.barre.y1 + t.barre.y2) / 2 : y) + 4);
      f.shadow.setAttribute('rx', t.barre && !lifting ? 9 : width / 2 + 2); f.shadow.setAttribute('ry', t.barre && !lifting ? (t.barre.y2 - t.barre.y1) / 2 : width / 2);
      f.shadow.style.opacity = pressed ? '.45' : '0';
    }

    let lastShape = null, lastWs = 1;
    const remember = (fn) => (shape, ws, immediate) => { lastShape = shape; lastWs = ws; fn(shape, ws, immediate); };
    window.addEventListener('resize', () => { if (lastShape) { for (const n in fingers) fingers[n].cur = null; palmCur = null; setShape(lastShape, lastWs, true); } });
    return { setShape: remember(setShape), state: () => Object.fromEntries(Object.entries(fingers).map(([n, f]) => [n, { placed: f.g.dataset.placed === '1', string: f.g.dataset.string, fret: f.g.dataset.fret, barre: f.g.classList.contains('is-barre') }])) };
  }
  return { create };
})();
