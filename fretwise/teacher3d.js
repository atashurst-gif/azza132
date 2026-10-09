/* Fretwise 3D teacher: a seated guitarist playing a Yamaha F310-style dreadnought.
   Everything the teacher does comes from the lesson timeline: the left hand is solved with inverse kinematics so each
   fingertip lands just behind the correct physical fret on the correct string, and the pick crosses each string at the
   exact moment its note is scheduled. Units are metres. */
import * as THREE from 'three';
import { GLTFLoader } from 'three/addons/loaders/GLTFLoader.js';
import { RoomEnvironment } from 'three/addons/environments/RoomEnvironment.js';

const V = (x = 0, y = 0, z = 0) => new THREE.Vector3(x, y, z);
const clamp = (v, a, b) => Math.max(a, Math.min(b, v));
const smooth = (k) => k <= 0 ? 0 : k >= 1 ? 1 : k * k * (3 - 2 * k);

/* ------------------------------------------------------------------ guitar ------------------------------------------------------------------ */
// Local frame: origin at the nut on the centre line of the fretboard surface plane (z=0 is the body top);
// +x runs along the strings towards the bridge, +y towards the HIGH e side, +z out of the top towards the audience.
export const GUITAR = {
  scale: 0.634,            // F310 scale length
  nutWidth: 0.043, width12: 0.052,
  spanNut: 0.0355, spanSaddle: 0.054,
  boardTop: 0.006, fretTop: 0.0072,
  frets: 20, bodyStartFret: 14, bodyLength: 0.505, depth: 0.105,
  soundholeX: 0.49, soundholeR: 0.05,
};
const fretX = (n) => GUITAR.scale - GUITAR.scale / Math.pow(2, n / 12);
GUITAR.fretX = fretX;
GUITAR.bodyStart = fretX(GUITAR.bodyStartFret) - 0.004;
const boardWidth = (x) => GUITAR.nutWidth + (GUITAR.width12 - GUITAR.nutWidth) * (x / fretX(12));
const span = (x) => GUITAR.spanNut + (GUITAR.spanSaddle - GUITAR.spanNut) * (x / GUITAR.scale);
const stringY = (s, x) => span(x) / 2 * (-1 + 2 * s / 5);         // s: 0 = low E (top when held) … 5 = high e
const stringZ = (x) => 0.0088 + 0.0027 * (x / GUITAR.scale);       // nut 8.8 mm → saddle 11.5 mm above the top
GUITAR.stringY = stringY; GUITAR.stringZ = stringZ;
/* Where a fingertip presses: just behind the fret wire (a quarter of the way back into the fret space). */
export function pressPoint(s, physFret, back = 0.26) {
  const x = physFret <= 0 ? 0 : fretX(physFret) - back * (fretX(physFret) - fretX(physFret - 1));
  return V(x, stringY(s, x), GUITAR.fretTop + 0.0068);
}

function bodyOutline() {
  // dreadnought half-outline as (distance from neck joint, half width); mirrored for the other side
  const pts = [[0, 0.052], [0.012, 0.098], [0.035, 0.128], [0.075, 0.143], [0.12, 0.146], [0.17, 0.139], [0.215, 0.135],
    [0.26, 0.146], [0.31, 0.178], [0.37, 0.197], [0.42, 0.192], [0.465, 0.163], [0.493, 0.11], [0.503, 0.05], [0.505, 0]];
  const curve = new THREE.SplineCurve(pts.map(([x, y]) => new THREE.Vector2(x, y)));
  const half = curve.getPoints(70);
  const full = [...half, ...half.slice(0, -1).reverse().map(p => new THREE.Vector2(p.x, -p.y))];
  return full.map(p => new THREE.Vector2(p.x + GUITAR.bodyStart, p.y));
}
function canvasTexture(w, h, draw) {
  const c = document.createElement('canvas'); c.width = w; c.height = h; const g = c.getContext('2d'); draw(g, w, h);
  const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace; t.anisotropy = 4; return t;
}
function spruceTexture() {
  return canvasTexture(1024, 512, (g, w, h) => {
    g.fillStyle = '#e9d3a6'; g.fillRect(0, 0, w, h);
    for (let i = 0; i < 260; i++) { const y = Math.random() * h; g.strokeStyle = `rgba(${150 + Math.random() * 40},${110 + Math.random() * 30},${60},${0.05 + Math.random() * 0.08})`; g.lineWidth = 0.6 + Math.random() * 1.6; g.beginPath(); g.moveTo(0, y); for (let x = 0; x <= w; x += 64) g.lineTo(x, y + Math.sin(x / 140 + i) * 1.5); g.stroke(); }
  });
}
function rosewoodTexture() {
  return canvasTexture(1024, 128, (g, w, h) => {
    g.fillStyle = '#3a2418'; g.fillRect(0, 0, w, h);
    for (let i = 0; i < 160; i++) { const y = Math.random() * h; g.strokeStyle = `rgba(${20 + Math.random() * 30},${10 + Math.random() * 15},${5},${0.25 + Math.random() * 0.3})`; g.lineWidth = 0.5 + Math.random() * 2; g.beginPath(); g.moveTo(0, y); g.lineTo(w, y + (Math.random() - 0.5) * 6); g.stroke(); }
  });
}
function mahoganyTexture() {
  return canvasTexture(512, 256, (g, w, h) => {
    g.fillStyle = '#7a4a26'; g.fillRect(0, 0, w, h);
    for (let i = 0; i < 120; i++) { const y = Math.random() * h; g.strokeStyle = `rgba(${60 + Math.random() * 40},${30 + Math.random() * 20},${10},${0.15 + Math.random() * 0.2})`; g.lineWidth = 0.5 + Math.random() * 2; g.beginPath(); g.moveTo(0, y); g.lineTo(w, y + (Math.random() - 0.5) * 10); g.stroke(); }
  });
}

/* Loft a tapered neck with a C-shaped back between the nut and the heel. */
function neckGeometry() {
  const x0 = -0.004, x1 = GUITAR.bodyStart + 0.012, segX = 24, segA = 18, pos = [], idx = [];
  for (let i = 0; i <= segX; i++) {
    const x = x0 + (x1 - x0) * i / segX; const half = boardWidth(Math.max(0, x)) / 2; const depth = 0.020 + 0.004 * clamp(x / fretX(12), 0, 1.3);
    for (let j = 0; j <= segA; j++) { const a = Math.PI * j / segA; pos.push(x, -Math.cos(a) * half, -Math.sin(a) * depth); }
  }
  for (let i = 0; i < segX; i++) for (let j = 0; j < segA; j++) { const a = i * (segA + 1) + j, b = a + segA + 1; idx.push(a, b, a + 1, b, b + 1, a + 1); }
  const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3)); g.setIndex(idx); g.computeVertexNormals(); return g;
}
/* Tapered fretboard slab from just behind the nut to fret 20. */
function boardGeometry() {
  const x0 = -0.001, x1 = fretX(GUITAR.frets) + 0.006, w0 = boardWidth(0) / 2, w1 = boardWidth(x1) / 2, t = GUITAR.boardTop;
  const v = [[x0, -w0, 0], [x1, -w1, 0], [x1, w1, 0], [x0, w0, 0], [x0, -w0, t], [x1, -w1, t], [x1, w1, t], [x0, w0, t]];
  const faces = [[4, 5, 6, 7], [0, 3, 2, 1], [0, 1, 5, 4], [2, 3, 7, 6], [1, 2, 6, 5], [0, 4, 7, 3]];
  const pos = [], uv = [];
  for (const f of faces) { const [a, b, c, d] = f.map(i => v[i]); for (const p of [a, b, c, a, c, d]) { pos.push(...p); uv.push((p[0] - x0) / (x1 - x0), (p[1] + w1) / (2 * w1)); } }
  const g = new THREE.BufferGeometry(); g.setAttribute('position', new THREE.Float32BufferAttribute(pos, 3)); g.setAttribute('uv', new THREE.Float32BufferAttribute(uv, 2)); g.computeVertexNormals(); return g;
}

export function buildGuitar() {
  const group = new THREE.Group(); group.name = 'guitar';
  const mat = {
    top: new THREE.MeshStandardMaterial({ map: spruceTexture(), color: 0xd8c08e, roughness: 0.5, metalness: 0, envMapIntensity: 0.3 }),
    sides: new THREE.MeshStandardMaterial({ map: mahoganyTexture(), roughness: 0.5, color: 0xc89a6a, envMapIntensity: 0.4 }),
    binding: new THREE.MeshStandardMaterial({ color: 0x111111, roughness: 0.3 }),
    neck: new THREE.MeshStandardMaterial({ map: mahoganyTexture(), roughness: 0.45 }),
    board: new THREE.MeshStandardMaterial({ map: rosewoodTexture(), roughness: 0.62 }),
    fret: new THREE.MeshStandardMaterial({ color: 0xd8d8d0, metalness: 0.9, roughness: 0.25 }),
    bone: new THREE.MeshStandardMaterial({ color: 0xf3eee0, roughness: 0.4 }),
    black: new THREE.MeshStandardMaterial({ color: 0x0c0c0c, roughness: 0.35 }),
    guard: new THREE.MeshStandardMaterial({ color: 0x080808, roughness: 0.18, metalness: 0.05 }),
    chrome: new THREE.MeshStandardMaterial({ color: 0xe6e6e6, metalness: 1, roughness: 0.18 }),
    bronze: new THREE.MeshStandardMaterial({ color: 0xc8a463, metalness: 0.9, roughness: 0.32 }),
    steel: new THREE.MeshStandardMaterial({ color: 0xdcdcdc, metalness: 0.95, roughness: 0.2 }),
    hole: new THREE.MeshStandardMaterial({ color: 0x0b0806, roughness: 1 }),
  };
  // body: extruded outline (sides + back), separate top face with spruce, black binding ring
  const outline = bodyOutline(); const shape = new THREE.Shape(outline);
  const hole = new THREE.Path(); hole.absarc(GUITAR.soundholeX, 0, GUITAR.soundholeR, 0, Math.PI * 2, true);
  const bodyGeo = new THREE.ExtrudeGeometry(new THREE.Shape(outline), { depth: GUITAR.depth, bevelEnabled: true, bevelThickness: 0.004, bevelSize: 0.004, bevelSegments: 3, curveSegments: 6 });
  bodyGeo.translate(0, 0, -GUITAR.depth - 0.0055);   // front cap sits 2 mm below the spruce top (no z-fighting)
  const body = new THREE.Mesh(bodyGeo, mat.sides); group.add(body);
  const topShape = new THREE.Shape(outline); topShape.holes.push(hole);
  const topGeo = new THREE.ShapeGeometry(topShape, 24); const tb = new THREE.Box3().setFromBufferAttribute(topGeo.attributes.position);
  const uv = topGeo.attributes.uv; for (let i = 0; i < uv.count; i++) { const x = topGeo.attributes.position.getX(i), y = topGeo.attributes.position.getY(i); uv.setXY(i, (x - tb.min.x) / (tb.max.x - tb.min.x), (y - tb.min.y) / (tb.max.y - tb.min.y)); }
  const top = new THREE.Mesh(topGeo, mat.top); top.position.z = 0.0035; group.add(top);
  const bindPts = outline.map(p => new THREE.Vector3(p.x, p.y, 0.0036)); bindPts.push(bindPts[0].clone());
  const binding = new THREE.Mesh(new THREE.TubeGeometry(new THREE.CatmullRomCurve3(bindPts, true), 280, 0.0028, 6, true), mat.binding); group.add(binding);
  // sound hole: dark disc inside, rosette rings
  const holeTex = canvasTexture(256, 256, (g, w, h) => { const gr = g.createRadialGradient(w / 2, h / 2, 10, w / 2, h / 2, w / 2); gr.addColorStop(0, '#2a1b10'); gr.addColorStop(0.7, '#120b06'); gr.addColorStop(1, '#050302'); g.fillStyle = gr; g.fillRect(0, 0, w, h); });
  const holeDisc = new THREE.Mesh(new THREE.CircleGeometry(GUITAR.soundholeR, 48), new THREE.MeshBasicMaterial({ map: holeTex })); holeDisc.position.set(GUITAR.soundholeX, 0, -0.0008); group.add(holeDisc);
  for (const [r, w, m] of [[0.0565, 0.0012, mat.black], [0.06, 0.0025, mat.black], [0.0645, 0.0011, mat.black], [0.0515, 0.0008, mat.black]]) { const ring = new THREE.Mesh(new THREE.RingGeometry(r - w, r + w, 64), m); ring.position.set(GUITAR.soundholeX, 0, 0.0037); group.add(ring); }
  // pickguard: Yamaha-style teardrop on the treble side below the sound hole
  const pg = new THREE.Shape(); const cx = GUITAR.soundholeX + 0.02;
  pg.moveTo(cx - 0.055, 0.058); pg.bezierCurveTo(cx - 0.06, 0.1, cx + 0.03, 0.155, cx + 0.085, 0.13); pg.bezierCurveTo(cx + 0.125, 0.1, cx + 0.09, 0.05, cx + 0.05, 0.05);
  pg.absarc(GUITAR.soundholeX, 0, 0.068, Math.atan2(0.05, cx + 0.05 - GUITAR.soundholeX), Math.atan2(0.058, cx - 0.055 - GUITAR.soundholeX), false);
  const guard = new THREE.Mesh(new THREE.ShapeGeometry(pg, 24), mat.guard); guard.position.z = 0.0039; group.add(guard);
  // bridge, saddle, bridge pins
  const bridge = new THREE.Mesh(new THREE.BoxGeometry(0.03, 0.15, 0.009), mat.black); bridge.position.set(GUITAR.scale + 0.006, 0, 0.0035 + 0.0045); group.add(bridge);
  const bridgeWing = new THREE.Mesh(new THREE.CylinderGeometry(0.015, 0.015, 0.17, 24, 1, false, 0, Math.PI), mat.black); bridgeWing.rotation.set(0, 0, 0); bridgeWing.scale.set(1, 1, 0.3); bridgeWing.position.set(GUITAR.scale + 0.006, 0, 0.0036); group.add(bridgeWing);
  const saddle = new THREE.Mesh(new THREE.BoxGeometry(0.003, 0.075, 0.004), mat.bone); saddle.position.set(GUITAR.scale, 0, 0.0035 + 0.009 + 0.0015); group.add(saddle);
  for (let s = 0; s < 6; s++) { const pin = new THREE.Mesh(new THREE.CylinderGeometry(0.0028, 0.0028, 0.004, 14), mat.bone); pin.rotation.x = Math.PI / 2; pin.position.set(GUITAR.scale + 0.013, stringY(s, GUITAR.scale) * 1.02, 0.0035 + 0.0105); group.add(pin); }
  // neck, fretboard, frets, nut, inlays
  const neck = new THREE.Mesh(neckGeometry(), mat.neck); group.add(neck);
  const heel = new THREE.Mesh(new THREE.CylinderGeometry(0.03, 0.022, 0.07, 20), mat.neck); heel.rotation.x = Math.PI / 2; heel.position.set(GUITAR.bodyStart + 0.004, 0, -0.04); heel.scale.set(1.0, 1, 1); group.add(heel);
  const board = new THREE.Mesh(boardGeometry(), mat.board); group.add(board);
  for (let n = 1; n <= GUITAR.frets; n++) { const x = fretX(n); const w = boardWidth(x); const f = new THREE.Mesh(new THREE.CylinderGeometry(0.0011, 0.0011, w, 8), mat.fret); f.position.set(x, 0, GUITAR.boardTop + 0.0004); group.add(f); }
  const nut = new THREE.Mesh(new THREE.BoxGeometry(0.005, GUITAR.nutWidth, 0.0045), mat.bone); nut.position.set(-0.0025, 0, GUITAR.boardTop + 0.0012); group.add(nut);
  for (const n of [3, 5, 7, 9, 12, 15, 17]) {
    const x = (fretX(n) + fretX(n - 1)) / 2; const ys = n === 12 ? [-0.012, 0.012] : [0];
    for (const y of ys) { const d = new THREE.Mesh(new THREE.CircleGeometry(0.0029, 20), mat.bone); d.position.set(x, y, GUITAR.boardTop + 0.0002); group.add(d); }
  }
  // headstock: angled back 14°, brown face with the Yamaha logo, 3+3 chrome tuners
  const head = new THREE.Group(); head.position.set(-0.004, 0, GUITAR.boardTop - 0.004); head.rotation.y = -THREE.MathUtils.degToRad(14); group.add(head);
  const hs = new THREE.Shape(); hs.moveTo(0, -0.026); hs.lineTo(-0.03, -0.038); hs.lineTo(-0.175, -0.041); hs.quadraticCurveTo(-0.192, -0.041, -0.19, -0.02); hs.lineTo(-0.192, 0.02); hs.quadraticCurveTo(-0.192, 0.041, -0.175, 0.041); hs.lineTo(-0.03, 0.038); hs.lineTo(0, 0.026); hs.lineTo(0, -0.026);
  const hsGeo = new THREE.ExtrudeGeometry(hs, { depth: 0.014, bevelEnabled: true, bevelThickness: 0.001, bevelSize: 0.001, bevelSegments: 1 }); hsGeo.translate(0, 0, -0.014);
  const logo = canvasTexture(512, 128, (g, w, h) => { g.fillStyle = '#8a5530'; g.fillRect(0, 0, w, h); g.fillStyle = '#f4efe2'; g.font = 'bold 54px Arial, Helvetica, sans-serif'; g.textAlign = 'center'; g.textBaseline = 'middle'; g.fillText('YAMAHA', w / 2, h / 2 + 2); });
  const hsMat = new THREE.MeshStandardMaterial({ map: mahoganyTexture(), roughness: 0.4, color: 0xd9a070 });
  const headstock = new THREE.Mesh(hsGeo, hsMat); head.add(headstock);
  const logoPlane = new THREE.Mesh(new THREE.PlaneGeometry(0.055, 0.0137), new THREE.MeshStandardMaterial({ map: logo, roughness: 0.4 })); logoPlane.position.set(-0.165, 0, 0.0012); logoPlane.rotation.z = -Math.PI / 2; head.add(logoPlane);
  for (let i = 0; i < 3; i++) for (const side of [-1, 1]) {
    const x = -0.06 - i * 0.042; const post = new THREE.Mesh(new THREE.CylinderGeometry(0.0035, 0.004, 0.012, 14), mat.chrome); post.rotation.x = Math.PI / 2; post.position.set(x, side * 0.026, 0.006); head.add(post);
    const bushing = new THREE.Mesh(new THREE.CylinderGeometry(0.006, 0.006, 0.002, 18), mat.chrome); bushing.rotation.x = Math.PI / 2; bushing.position.set(x, side * 0.026, 0.0012); head.add(bushing);
    const shaft = new THREE.Mesh(new THREE.CylinderGeometry(0.002, 0.002, 0.024, 8), mat.chrome); shaft.position.set(x, side * 0.05, -0.008); head.add(shaft);
    const button = new THREE.Mesh(new THREE.BoxGeometry(0.016, 0.006, 0.019), mat.chrome); button.position.set(x, side * 0.064, -0.008); head.add(button);
    const housing = new THREE.Mesh(new THREE.BoxGeometry(0.02, 0.016, 0.012), mat.chrome); housing.position.set(x, side * 0.044, -0.019); head.add(housing);
  }
  // strings: each a tube from nut to saddle; the middle can be displaced to show vibration
  const strings = [];
  const radii = [0.00068, 0.00058, 0.00048, 0.00040, 0.00032, 0.00027];
  for (let s = 0; s < 6; s++) {
    const seg = 40; const geo = new THREE.CylinderGeometry(radii[s], radii[s], 1, 6, seg, true); geo.rotateZ(-Math.PI / 2); geo.translate(0.5, 0, 0);
    const m = new THREE.Mesh(geo, s < 4 ? mat.bronze : mat.steel); m.userData.base = geo.attributes.position.array.slice(); m.frustumCulled = false; group.add(m);
    strings.push({ mesh: m, amp: 0, phase: 0, freq: 30 + s * 6, s });
    // string past the nut to its tuner post (visual only)
    const tunerSide = s < 3 ? -1 : 1; const ti = s < 3 ? 2 - s : s - 3; const postLocal = V(-0.06 - ti * 0.042, tunerSide * 0.026, 0.006);
    const postWorld = postLocal.clone().applyEuler(head.rotation).add(head.position);
    const a = V(-0.004, stringY(s, 0), stringZ(0)); const len = a.distanceTo(postWorld); const tail = new THREE.Mesh(new THREE.CylinderGeometry(radii[s], radii[s], len, 5), s < 4 ? mat.bronze : mat.steel);
    tail.position.copy(a.clone().add(postWorld).multiplyScalar(0.5)); tail.quaternion.setFromUnitVectors(V(0, 1, 0), postWorld.clone().sub(a).normalize()); group.add(tail);
  }
  function layoutStrings(pressed) {
    // pressed: array per string of physical fret pressed (0/-1 = open). A pressed string bends down to the fret it is fretted at.
    for (const st of strings) {
      const { mesh, s } = st; const arr = mesh.geometry.attributes.position.array; const base = mesh.userData.base;
      const pf = pressed ? pressed[s] : 0; const xp = pf > 0 ? fretX(pf) : 0;
      for (let i = 0; i < arr.length; i += 3) {
        const u = base[i]; const x = u * GUITAR.scale; let z = stringZ(x);
        if (pf > 0) { const zf = GUITAR.fretTop + 0.0004; if (x <= xp) z = Math.min(z, GUITAR.boardTop + 0.0006 + (zf - GUITAR.boardTop - 0.0006) * (x / xp)); else z = zf + (stringZ(GUITAR.scale) - zf) * (x - xp) / (GUITAR.scale - xp); }
        const lo = pf > 0 ? xp : 0; const env = x > lo ? Math.sin(Math.PI * (x - lo) / (GUITAR.scale - lo)) : 0;
        const wob = st.amp * env * Math.sin(st.phase) * 0.0016;
        arr[i] = x; arr[i + 1] = stringY(s, x) + base[i + 1] + wob * 0.35; arr[i + 2] = z + base[i + 2] + wob;
      }
      mesh.geometry.attributes.position.needsUpdate = true;
    }
  }
  layoutStrings(null);
  // capo: a slim black bar with a rubber pad lying across the strings just behind the capo fret
  const capo = new THREE.Group(); const capoMat = new THREE.MeshStandardMaterial({ color: 0x1b1b1b, metalness: 0.35, roughness: 0.4 });
  const capoBar = new THREE.Mesh(new THREE.CapsuleGeometry(0.0034, 0.05, 6, 12), capoMat); capoBar.position.z = 0.0135; capo.add(capoBar);
  const capoPad = new THREE.Mesh(new THREE.BoxGeometry(0.007, 0.05, 0.0025), new THREE.MeshStandardMaterial({ color: 0x2b2b2b, roughness: 0.9 })); capoPad.position.z = 0.0105; capo.add(capoPad);
  capo.visible = false; group.add(capo);
  function setCapo(n) { capo.visible = n > 0; if (n > 0) { const x = fretX(n) - 0.0055; capo.position.set(x, 0, 0); const w = boardWidth(x) + 0.006; capoBar.scale.set(1, w / 0.0568, 1); capoPad.scale.y = w / 0.05; } }
  // optional hint dots, hidden by default
  const hints = new THREE.Group(); group.add(hints);
  const hintMat = new THREE.MeshBasicMaterial({ color: 0x8ef0a6, transparent: true, opacity: 0.9, depthTest: false });
  function showHints(points) { hints.clear(); if (!points) return; for (const p of points) { const d = new THREE.Mesh(new THREE.CircleGeometry(0.0042, 20), hintMat); d.position.set(p.x, p.y, GUITAR.fretTop + 0.004); d.renderOrder = 10; hints.add(d); } }
  return { group, strings, layoutStrings, setCapo, showHints, materials: mat };
}

/* ------------------------------------------------------------------ IK helpers ------------------------------------------------------------------ */
const _q1 = new THREE.Quaternion(), _q2 = new THREE.Quaternion();
const wpos = (o) => o.getWorldPosition(V());
const wquat = (o) => o.getWorldQuaternion(new THREE.Quaternion());
function setWorldQuat(bone, q) { bone.quaternion.copy(wquat(bone.parent).invert().multiply(q)); bone.updateMatrixWorld(true); }
function rotateWorld(bone, delta) { setWorldQuat(bone, delta.clone().multiply(wquat(bone))); }
function aim(bone, child, target) { const p = wpos(bone); const from = wpos(child).sub(p).normalize(); const to = target.clone().sub(p).normalize(); if (from.lengthSq() < 1e-9 || to.lengthSq() < 1e-9) return; rotateWorld(bone, new THREE.Quaternion().setFromUnitVectors(from, to)); }
function basisQuat(fwd, up) { // rotation whose columns are (fwd, up', fwd×up')
  const f = fwd.clone().normalize(); const u = up.clone().sub(f.clone().multiplyScalar(up.dot(f))).normalize(); const s = new THREE.Vector3().crossVectors(f, u);
  return new THREE.Quaternion().setFromRotationMatrix(new THREE.Matrix4().makeBasis(f, u, s));
}
/* Two-bone IK: place `end` at target with the middle joint bending towards `pole`. */
function twoBone(a, b, c, target, pole) {
  const pa = wpos(a), pb = wpos(b), pc = wpos(c); const l1 = pa.distanceTo(pb), l2 = pb.distanceTo(pc);
  const t = target.clone(); let d = t.distanceTo(pa); const dmax = l1 + l2 - 1e-4, dmin = Math.abs(l1 - l2) + 1e-4;
  const dir = t.clone().sub(pa).normalize(); if (d > dmax) { d = dmax; t.copy(pa).addScaledVector(dir, d); } if (d < dmin) { d = dmin; t.copy(pa).addScaledVector(dir, d); }
  const cosA = clamp((l1 * l1 + d * d - l2 * l2) / (2 * l1 * d), -1, 1); const ang = Math.acos(cosA);
  const toPole = pole.clone().sub(pa); let bend = toPole.sub(dir.clone().multiplyScalar(toPole.dot(dir)));
  if (bend.lengthSq() < 1e-8) { bend = V(0, -1, 0).sub(dir.clone().multiplyScalar(-dir.y)); if (bend.lengthSq() < 1e-8) bend = V(0, 0, -1); }  // pole collinear: bend downwards
  bend.normalize();
  const elbow = pa.clone().addScaledVector(dir, Math.cos(ang) * l1).addScaledVector(bend, Math.sin(ang) * l1);
  aim(a, b, elbow); aim(b, c, t);
}
/* Planar three-joint finger: DIP flexes ~0.75× PIP (tendon coupling). Bends towards the palm normal. */
function curlDistance(L, b, k) { const a1 = 0, a2 = b, a3 = b + k * b; const x = L[0] * Math.cos(a1) + L[1] * Math.cos(a2) + L[2] * Math.cos(a3); const y = L[0] * Math.sin(a1) + L[1] * Math.sin(a2) + L[2] * Math.sin(a3); return Math.hypot(x, y); }
function chainEnd(L, a, b, k) { // planar tip position for MCP flex a, PIP b, DIP k*b
  const t1 = a, t2 = a + b, t3 = a + b + k * b;
  return [L[0] * Math.cos(t1) + L[1] * Math.cos(t2) + L[2] * Math.cos(t3), L[0] * Math.sin(t1) + L[1] * Math.sin(t2) + L[2] * Math.sin(t3)];
}
/* Anatomical finger IK: spread (abduct) at the knuckle about the palm normal, then flex knuckle / middle / end joints in
   the finger's own plane, with the end joint following the middle joint (tendon coupling). Returns the joint angles. */
function solveFinger(chain, rest, target, palmN, opts = {}) {
  chain.forEach((bone, i) => { if (i < 3) bone.quaternion.copy(rest[i]); }); chain[0].updateMatrixWorld(true);
  const P = chain.map(wpos); const L = [P[0].distanceTo(P[1]), P[1].distanceTo(P[2]), P[2].distanceTo(P[3])];
  let d = P[3].clone().sub(P[0]).normalize(); const n = palmN.clone().sub(d.clone().multiplyScalar(palmN.dot(d))).normalize();
  const v = target.clone().sub(P[0]);
  // 1. spread so the finger's bending plane contains the target
  const vp = v.clone().sub(n.clone().multiplyScalar(v.dot(n)));
  let spread = 0;
  if (vp.lengthSq() > 1e-8) { const u = vp.normalize(); spread = Math.atan2(new THREE.Vector3().crossVectors(d, u).dot(n), d.dot(u)); spread = clamp(spread, -(opts.maxSpread ?? 0.5), opts.maxSpread ?? 0.5); }
  if (spread) { rotateWorld(chain[0], new THREE.Quaternion().setFromAxisAngle(n, spread)); d.applyAxisAngle(n, spread); }
  const k = opts.coupling ?? 0.72; const axis = new THREE.Vector3().crossVectors(d, n).normalize();
  // 2. middle-joint bend from the distance to the target (distance does not depend on the knuckle angle)
  const D = v.length(); let bend = 0; const maxB = opts.maxCurl ?? 1.75;
  if (!opts.straight) { const dist = (bb) => Math.hypot(...chainEnd(L, 0, bb, k)); if (dist(0) <= D) bend = 0; else if (dist(maxB) >= D) bend = maxB; else { let lo = 0, hi = maxB; for (let i = 0; i < 28; i++) { const m = (lo + hi) / 2; if (dist(m) > D) lo = m; else hi = m; } bend = (lo + hi) / 2; } }
  // 3. knuckle flex points the chain at the target inside the bending plane
  const [ex, ey] = chainEnd(L, 0, bend, k); const tx = v.dot(d), ty = v.dot(n);
  let flex = Math.atan2(ty, tx) - Math.atan2(ey, ex); flex = clamp(flex, opts.minFlex ?? -0.45, opts.maxFlex ?? 1.65);
  rotateWorld(chain[0], new THREE.Quaternion().setFromAxisAngle(axis, flex));
  if (bend) { rotateWorld(chain[1], new THREE.Quaternion().setFromAxisAngle(axis, bend)); rotateWorld(chain[2], new THREE.Quaternion().setFromAxisAngle(axis, bend * k)); }
  return { spread, flex, bend, reach: L[0] + L[1] + L[2] };
}

/* Curl a finger by fixed joint angles (radians) towards the palm — used for fingers that are not pressing a string. */
function curlFinger(chain, rest, palmN, flex, bend, coupling = 0.75) {
  chain.forEach((bone, i) => { if (i < 3) bone.quaternion.copy(rest[i]); }); chain[0].updateMatrixWorld(true);
  const P0 = wpos(chain[0]), P3 = wpos(chain[3]); const d = P3.sub(P0).normalize(); const n = palmN.clone().sub(d.clone().multiplyScalar(palmN.dot(d))).normalize();
  const axis = new THREE.Vector3().crossVectors(d, n).normalize();
  rotateWorld(chain[0], new THREE.Quaternion().setFromAxisAngle(axis, flex));
  rotateWorld(chain[1], new THREE.Quaternion().setFromAxisAngle(axis, bend));
  rotateWorld(chain[2], new THREE.Quaternion().setFromAxisAngle(axis, bend * coupling));
}

/* ------------------------------------------------------------------ hand proportions ------------------------------------------------------------------ */
/* The stylised character has fingers whose middle and tip segments are almost as long as the base segment (human ratio is
   roughly 1 : 0.64 : 0.41). Re-proportion each finger at load time, in the rest (T) pose:
   - every skinned vertex is moved along its own segment's axis, scaled to the new segment length (weights respected);
   - the child bones are moved to the new joint positions; the skeleton's inverse bind matrices are recalculated.
   Segment ratios follow human hand anthropometry (Buchholz, Armstrong & Goldstein 1992). `shorten` is the overall finger
   length relative to the original model. */
export const HAND_PROPORTIONS = {
  Index: { ratios: [1, 0.58, 0.40], shorten: 0.86 },
  Middle: { ratios: [1, 0.64, 0.41], shorten: 0.84 },
  Ring: { ratios: [1, 0.68, 0.44], shorten: 0.85 },
  Pinky: { ratios: [1, 0.57, 0.46], shorten: 0.86 },
  Thumb: { ratios: [1, 0.78, 0.63], shorten: 0.90 },
};
export function reproportionHands(root, bones, props = HAND_PROPORTIONS) {
  root.updateMatrixWorld(true);
  const meshes = []; root.traverse(o => { if (o.isSkinnedMesh) meshes.push(o); });
  const report = {};
  for (const side of ['Left', 'Right']) for (const [f, cfg] of Object.entries(props)) {
    const ch = [1, 2, 3, 4].map(i => bones[`${side}Hand${f}${i}`]); if (ch.some(b => !b)) continue;
    // segment lengths come from the loaded (node) pose; both poses share bone lengths
    const Jn = ch.map(b => b.getWorldPosition(new THREE.Vector3()));
    const L = [Jn[0].distanceTo(Jn[1]), Jn[1].distanceTo(Jn[2]), Jn[2].distanceTo(Jn[3])];
    const total = (L[0] + L[1] + L[2]) * cfg.shorten; const rs = cfg.ratios.reduce((a, b) => a + b, 0);
    const L2 = cfg.ratios.map(r => total * r / rs);
    report[`${side}${f}`] = { before: L.map(x => +(x * 1000).toFixed(1)), after: L2.map(x => +(x * 1000).toFixed(1)) };
    for (const mesh of meshes) {
      const skel = mesh.skeleton; const idx = ch.map(b => skel.bones.indexOf(b)); if (idx.some(i => i < 0)) continue;
      // work in BIND space: bone bind matrices are the inverses of skeleton.boneInverses; vertices are bindMatrix × position
      const bindW = idx.map(i => skel.boneInverses[i].clone().invert());
      const J = bindW.map(m => new THREE.Vector3().setFromMatrixPosition(m));
      const U = [0, 1, 2].map(k => J[k + 1].clone().sub(J[k]).normalize());
      const Lb = [J[0].distanceTo(J[1]), J[1].distanceTo(J[2]), J[2].distanceTo(J[3])];
      const J2 = [J[0].clone()]; for (let k = 0; k < 3; k++) J2.push(J2[k].clone().addScaledVector(U[k], L2[k] * (Lb[k] / L[k])));
      const pos = mesh.geometry.attributes.position, si = mesh.geometry.attributes.skinIndex, sw = mesh.geometry.attributes.skinWeight;
      // note: in 'attached' bind mode three.js overwrites bindMatrixInverse every frame, so invert bindMatrix ourselves
      const toBind = mesh.bindMatrix, fromBind = mesh.bindMatrix.clone().invert(); const v = new THREE.Vector3(), out = new THREE.Vector3(), rel = new THREE.Vector3();
      for (let i = 0; i < pos.count; i++) {
        let wSum = 0; out.set(0, 0, 0);
        for (let c = 0; c < 4; c++) {
          const w = sw.getComponent(i, c); if (!w) continue; const k = idx.slice(0, 3).indexOf(si.getComponent(i, c)); if (k < 0) continue;
          if (!wSum) v.fromBufferAttribute(pos, i).applyMatrix4(toBind);
          // slide the vertex along its own segment, scaled to the new segment length; its distance from the bone axis is kept
          rel.copy(v).sub(J[k]); const a = rel.dot(U[k]); rel.addScaledVector(U[k], -a);
          out.addScaledVector(J2[k].clone().addScaledVector(U[k], a * (L2[k] * (Lb[k] / L[k])) / Lb[k]).add(rel), w); wSum += w;
        }
        if (!wSum) continue;
        out.addScaledVector(v, 1 - wSum).applyMatrix4(fromBind); pos.setXYZ(i, out.x, out.y, out.z);
      }
      pos.needsUpdate = true; mesh.geometry.computeBoundingSphere();
      // the moved joints get new bind matrices (same orientation, new origin); nothing else in the skeleton changes
      for (let k = 1; k < 4; k++) { const m = bindW[k].clone(); m.setPosition(J2[k]); skel.boneInverses[idx[k]].copy(m.invert()); }
    }
    // and the child bones move to the new joints in the loaded pose (local offsets point along the finger)
    for (let k = 1; k < 4; k++) ch[k].position.multiplyScalar(L2[k - 1] / L[k - 1]);
  }
  root.updateMatrixWorld(true);
  return report;
}

/* Copy the left hand + forearm out of the character's single skinned mesh into its own mesh that shares the skeleton,
   with a per-vertex fade towards the elbow. */
function extractFrettingHand(mesh, B) {
  const g = mesh.geometry, si = g.attributes.skinIndex, sw = g.attributes.skinWeight, bones = mesh.skeleton.bones;
  const set = new Set(Object.entries(B).filter(([n]) => /^LeftHand|^LeftForeArm$/.test(n)).map(([, b]) => b));
  const share = new Float32Array(si.count);
  for (let i = 0; i < si.count; i++) { let w = 0; for (let c = 0; c < 4; c++) if (set.has(bones[si.getComponent(i, c)])) w += sw.getComponent(i, c); share[i] = w; }
  const src = g.index ? g.index.array : Array.from({ length: si.count }, (_, i) => i); const keep = [];
  for (let i = 0; i < src.length; i += 3) { const a = src[i], b = src[i + 1], c = src[i + 2]; if (share[a] > 0.5 && share[b] > 0.5 && share[c] > 0.5) keep.push(a, b, c); }
  const geo = g.clone(); geo.setIndex(keep);
  // fade: 1 on the hand, falling to 0 two-thirds of the way up the forearm (bind space)
  const inv = (b) => mesh.skeleton.boneInverses[bones.indexOf(b)].clone().invert();
  const wrist = new THREE.Vector3().setFromMatrixPosition(inv(B.LeftHand)), elbow = new THREE.Vector3().setFromMatrixPosition(inv(B.LeftForeArm));
  const axis = elbow.clone().sub(wrist); const len = axis.length(); axis.normalize();
  const pos = geo.attributes.position, fade = new Float32Array(pos.count), v = new THREE.Vector3();
  for (let i = 0; i < pos.count; i++) { v.fromBufferAttribute(pos, i).applyMatrix4(mesh.bindMatrix); const t = v.sub(wrist).dot(axis) / len; fade[i] = 1 - smooth((t - 0.2) / 0.5); }
  geo.setAttribute('fade', new THREE.BufferAttribute(fade, 1));
  const mat = mesh.material.clone(); mat.transparent = true; mat.opacity = 0.82; mat.envMapIntensity = 0.5;   // slightly see-through so the strings under the fingers stay visible
  mat.onBeforeCompile = (sh) => {
    sh.vertexShader = sh.vertexShader.replace('#include <common>', '#include <common>\nattribute float fade;\nvarying float vFade;').replace('#include <begin_vertex>', '#include <begin_vertex>\nvFade = fade;');
    sh.fragmentShader = sh.fragmentShader.replace('#include <common>', '#include <common>\nvarying float vFade;').replace('#include <color_fragment>', '#include <color_fragment>\ndiffuseColor.a *= vFade;');
  };
  const hand = new THREE.SkinnedMesh(geo, mat); hand.name = 'frettingHand';
  hand.position.copy(mesh.position); hand.quaternion.copy(mesh.quaternion); hand.scale.copy(mesh.scale);
  hand.bind(mesh.skeleton, mesh.bindMatrix); hand.castShadow = true; hand.receiveShadow = true; hand.frustumCulled = false;
  mesh.parent.add(hand);
  return hand;
}

/* Finger colours used for the numbered fingertips and the strings they press (1 index … 4 little finger). */
export const FINGER_COLOURS = { 0: '#f4f1e6', 1: '#4fc3f7', 2: '#7be07a', 3: '#ffb547', 4: '#f2709c' };
function labelSprite(text, { fg = '#10151a', bg = null, ring = null, size = 0.008, bold = true } = {}) {
  const c = document.createElement('canvas'); c.width = c.height = 128; const g = c.getContext('2d');
  if (bg) { g.beginPath(); g.arc(64, 64, 56, 0, Math.PI * 2); g.fillStyle = bg; g.fill(); }
  if (ring) { g.beginPath(); g.arc(64, 64, 56, 0, Math.PI * 2); g.lineWidth = 10; g.strokeStyle = ring; g.stroke(); }
  g.fillStyle = fg; g.font = `${bold ? 'bold ' : ''}78px Arial, Helvetica, sans-serif`; g.textAlign = 'center'; g.textBaseline = 'middle'; g.fillText(text, 64, 70);
  const t = new THREE.CanvasTexture(c); t.colorSpace = THREE.SRGBColorSpace;
  const sp = new THREE.Sprite(new THREE.SpriteMaterial({ map: t, depthTest: false, transparent: true })); sp.scale.setScalar(size); sp.renderOrder = 20; return sp;
}

/* ------------------------------------------------------------------ the stage ------------------------------------------------------------------ */
export async function createTeacherStage(container, options = {}) {
  const renderer = new THREE.WebGLRenderer({ antialias: true, alpha: false, preserveDrawingBuffer: !!options.preserveDrawingBuffer, powerPreference: 'high-performance' });
  renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2)); renderer.outputColorSpace = THREE.SRGBColorSpace; renderer.toneMapping = THREE.ACESFilmicToneMapping; renderer.toneMappingExposure = 1.05; renderer.shadowMap.enabled = true; renderer.shadowMap.type = THREE.PCFSoftShadowMap;
  container.append(renderer.domElement); renderer.domElement.className = 'teacher-canvas'; renderer.domElement.style.width = '100%'; renderer.domElement.style.height = '100%'; renderer.domElement.style.display = 'block';
  // Adaptive quality: software renderers (no GPU) and slow machines get a lower resolution and no shadows.
  let quality = 'high'; try { const gl = renderer.getContext(); const ext = gl.getExtension('WEBGL_debug_renderer_info'); const name = ext ? gl.getParameter(ext.UNMASKED_RENDERER_WEBGL) : ''; if (/swiftshader|llvmpipe|software/i.test(name)) quality = 'low'; } catch (e) { }
  function applyQuality() { const low = quality === 'low'; renderer.setPixelRatio(low ? 0.6 : Math.min(window.devicePixelRatio || 1, 2)); renderer.shadowMap.enabled = !low; if (typeof resize === 'function') resize(); }
  const scene = new THREE.Scene();
  const pmrem = new THREE.PMREMGenerator(renderer); scene.environment = pmrem.fromScene(new RoomEnvironment(), 0.04).texture;
  // dark studio backdrop: a soft radial gradient behind the instrument (nothing else competes with the guitar)
  scene.background = canvasTexture(1024, 576, (g, w, h) => { const gr = g.createRadialGradient(w * 0.5, h * 0.42, 40, w * 0.5, h * 0.5, w * 0.7); gr.addColorStop(0, '#34443a'); gr.addColorStop(0.55, '#1c2621'); gr.addColorStop(1, '#0d1310'); g.fillStyle = gr; g.fillRect(0, 0, w, h); });
  const key = new THREE.DirectionalLight(0xfff3e2, 2.4); key.castShadow = true; key.shadow.mapSize.set(2048, 2048); key.shadow.bias = -0.0003; key.shadow.normalBias = 0.002; scene.add(key); scene.add(key.target);
  Object.assign(key.shadow.camera, { left: -0.6, right: 0.6, top: 0.6, bottom: -0.6, near: 0.5, far: 4 });
  const fill = new THREE.DirectionalLight(0xd8efe0, 0.8); scene.add(fill); scene.add(fill.target);
  scene.add(new THREE.HemisphereLight(0xfaf3e6, 0x2a221c, 0.7));

  const gltf = await new GLTFLoader().loadAsync(options.modelUrl || './assets/models/teacher.glb');
  const person = gltf.scene; scene.add(person);
  person.traverse(o => { if (o.isMesh) { o.castShadow = true; o.receiveShadow = true; o.frustumCulled = false; if (o.material) { o.material.envMapIntensity = 0.6; } } });
  const B = {}; person.traverse(o => { if (o.isBone) B[o.name.replace(/^mixamorig:?/, '')] = o; });
  const handReport = options.originalHands ? null : reproportionHands(person, B);
  // Only the fretting hand and forearm are shown. Triangles skinned to the left hand/forearm are copied into their own
  // mesh (sharing the skeleton); the forearm fades out towards the elbow so there is no hard cut. The body is hidden.
  const bodyMesh = (() => { let m = null; person.traverse(o => { if (o.isSkinnedMesh && !m) m = o; }); return m; })();
  const handMesh = extractFrettingHand(bodyMesh, B); bodyMesh.visible = false; bodyMesh.castShadow = false;
  const restQ = {}; for (const [n, b] of Object.entries(B)) restQ[n] = b.quaternion.clone();
  person.updateMatrixWorld(true);
  // palm normals (T-pose palms face down) and finger chains
  const palmLocal = {}; for (const side of ['Left', 'Right']) palmLocal[side] = V(0, -1, 0).applyQuaternion(wquat(B[side + 'Hand']).invert());
  const FINGERS = ['Thumb', 'Index', 'Middle', 'Ring', 'Pinky'];
  const chain = (side, f) => [1, 2, 3, 4].map(i => B[`${side}Hand${f}${i}`]);
  const restOf = (side, f) => [1, 2, 3].map(i => restQ[`${side}Hand${f}${i}`]);
  const handLocal = {}; // wrist→knuckle-centre offset and forward axis in hand-local space (from the rest pose)
  for (const side of ['Left', 'Right']) {
    const hq = wquat(B[side + 'Hand']).invert(); const hp = wpos(B[side + 'Hand']);
    const mcp = ['Index', 'Middle', 'Ring', 'Pinky'].map(f => wpos(B[`${side}Hand${f}1`])); const c = mcp.reduce((a, p) => a.add(p), V()).multiplyScalar(0.25);
    handLocal[side] = { knuckles: c.clone().sub(hp).applyQuaternion(hq), forward: c.clone().sub(hp).normalize().applyQuaternion(hq), across: mcp[3].clone().sub(mcp[0]).normalize().applyQuaternion(hq), palm: palmLocal[side].clone() };
  }
  const reset = () => { for (const [n, b] of Object.entries(B)) b.quaternion.copy(restQ[n]); };

  /* --- seated pose --- */
  const SEAT = 0.53; const hipsRestY = wpos(B.Hips).y;
  function seat() {
    reset(); const drop = hipsRestY - (SEAT + 0.075); person.position.set(0, -drop, 0); person.updateMatrixWorld(true);
    for (const side of ['Left', 'Right']) {
      const hip = wpos(B[side + 'UpLeg']); const sx = side === 'Left' ? 1 : -1;
      const knee = hip.clone().add(V(sx * 0.07, -0.015, 0.4)); const foot = V(knee.x + sx * 0.02, 0.085, knee.z + 0.05);
      twoBone(B[side + 'UpLeg'], B[side + 'Leg'], B[side + 'Foot'], foot, knee.clone().add(V(0, 0.1, 0.3)));
      aimFoot(side);
    }
    // lean slightly forward over the guitar
    rotateWorld(B.Spine, new THREE.Quaternion().setFromAxisAngle(V(1, 0, 0), 0.10));
    rotateWorld(B.Spine2, new THREE.Quaternion().setFromAxisAngle(V(1, 0, 0), 0.06));
    // relax shoulders
    rotateWorld(B.LeftShoulder, new THREE.Quaternion().setFromAxisAngle(V(0, 0, 1), -0.08));
    rotateWorld(B.RightShoulder, new THREE.Quaternion().setFromAxisAngle(V(0, 0, 1), 0.08));
  }
  function aimFoot(side) { const foot = B[side + 'Foot'], toe = B[side + 'ToeBase']; if (!toe) return; const p = wpos(foot); aim(foot, toe, p.clone().add(V(0, -0.06, 0.13))); }

  /* --- guitar placement --- */
  const guitar = buildGuitar(); scene.add(guitar.group); guitar.group.traverse(o => { if (o.isMesh) { o.castShadow = true; o.receiveShadow = true; } });
  const G = guitar.group;
  const ANG = { neckUp: 0.33, tiltBack: 0.2, yaw: 0.28 }; // radians: neck raised, top tilted towards the player's eyes, neck angled forward
  function placeGuitar() {
    seat(); person.updateMatrixWorld(true);
    const thigh = wpos(B.RightUpLeg).lerp(wpos(B.RightLeg), 0.5); const torso = wpos(B.Spine1);
    const q = new THREE.Quaternion().setFromEuler(new THREE.Euler(-ANG.tiltBack, ANG.yaw, Math.PI + ANG.neckUp, 'YXZ'));
    // Euler maps local +x (nut→bridge) to world -x (her right), local +y (high e side) to world down, local +z out of the top towards the camera
    G.quaternion.copy(q);
    // anchor: the waist on the treble (lower) edge rests on the right thigh
    const waistLocal = V(GUITAR.bodyStart + 0.215, 0.137, -GUITAR.depth * 0.55);
    const waistWorld = waistLocal.clone().applyQuaternion(q);
    G.position.copy(V(thigh.x + 0.02, thigh.y + 0.075, torso.z + 0.21)).sub(waistWorld);
    G.updateMatrixWorld(true);
  }
  const toWorld = (p) => p.clone().applyMatrix4(G.matrixWorld);
  const dirWorld = (d) => d.clone().transformDirection(G.matrixWorld);

  /* --- left hand: fingers on frets --- */
  let leftTargets = null, leftFrom = null, leftStart = 0, leftShape = null; const TWEEN = 0.13;
  function leftHandTargets(shape, capo) {
    // returns per finger (1..4) a target point in GUITAR LOCAL space, plus flags
    const out = {}; const frets = shape ? shape.frets : [-1, -1, -1, -1, -1, -1]; const fingers = shape ? shape.fingers : [0, 0, 0, 0, 0, 0];
    const barre = shape && shape.barre;
    const pressed = {};
    frets.forEach((f, s) => { const n = fingers[s]; if (f < 1 || !n) return; if (barre && n === 1) return; if (!pressed[n] || s > pressed[n].s) pressed[n] = { s, phys: f + capo }; });
    // where the hand sits: anchor fret under the first finger
    const used = Object.entries(pressed).map(([n, p]) => p.phys - (Number(n) - 1));
    let anchor = used.length ? Math.round(used.reduce((a, b) => a + b, 0) / used.length) : capo + 1; if (barre) anchor = barre.fret + capo;
    anchor = clamp(anchor, capo + 1, 15);
    for (let n = 1; n <= 4; n++) {
      if (barre && n === 1) {
        // a barre lays the first finger flat along the fret: its knuckle sits at the treble edge and, for a full barre,
        // the fingertip only just clears the bass edge; a partial barre ends just beyond its last string
        const x = pressPoint(0, barre.fret + capo).x; const full = barre.from === 0;
        const y = full ? -boardWidth(x) / 2 - 0.004 : stringY(barre.from - 0.7, x);
        out[1] = { p: V(x, y, GUITAR.fretTop + 0.0072), barre: true, pressed: true, s: barre.from, phys: barre.fret + capo }; continue;
      }
      if (pressed[n]) {
        // fingers sharing a fret line up diagonally (lowest-numbered finger furthest from the fret), as in an A chord
        const same = Object.entries(pressed).filter(([, q]) => q.phys === pressed[n].phys).map(([m]) => Number(m)).sort((a, b) => a - b);
        const backs = same.length >= 3 ? [0.5, 0.33, 0.16] : same.length === 2 ? [0.36, 0.2] : [0.26];
        out[n] = { p: pressPoint(pressed[n].s, pressed[n].phys, backs[same.indexOf(n)] ?? 0.26), pressed: true, ...pressed[n] }; continue;
      }
      // relaxed finger: hovers ~12 mm above the treble strings over its natural fret
      const phys = clamp(anchor + n - 1, 1, 19); const x = pressPoint(4, phys).x; out[n] = { p: V(x, stringY(4.6, x), GUITAR.fretTop + 0.016), pressed: false, phys };
    }
    out.anchor = anchor; out.thumb = V((fretX(anchor) + fretX(anchor + 1)) / 2 + 0.004, -0.004, -0.024);
    return out;
  }
  const names = { 1: 'Index', 2: 'Middle', 3: 'Ring', 4: 'Pinky' };
  /* Pose the whole left arm for a set of fingertip targets. hp = hand placement {ky, kz, pitch, slide}:
     knuckle line height below the treble edge, depth relative to the board, wrist pitch about the knuckle line and an
     offset along the neck. Returns the summed squared fingertip error (m²) for pressed fingers. */
  function solveLeft(t, hp) {
    const L = 'Left';
    const xs = [1, 2, 3, 4].map(n => t[n].p.x); const xMid = (Math.min(...xs) + Math.max(...xs)) / 2 + hp.slide;
    const knucklesLocal = V(xMid, boardWidth(xMid) / 2 + hp.ky, hp.kz);
    // wrist below and behind the neck, knuckles just past the treble edge, palm facing the neck: the fingers arch up
    // over the board and down onto the strings while the thumb rests behind the neck
    const base = V(0.06, -0.62, 0.78).normalize(); const palm0 = V(0.0, -0.78, -0.62).normalize();
    const pitchQ = new THREE.Quaternion().setFromAxisAngle(V(1, 0, 0), hp.pitch);
    const fwdW = dirWorld(base.clone().applyQuaternion(pitchQ)), palmW = dirWorld(palm0.clone().applyQuaternion(pitchQ));
    const hl = handLocal[L]; const handQ = basisQuat(fwdW, palmW).multiply(basisQuat(hl.forward, hl.palm).invert());
    const wrist = toWorld(knucklesLocal).sub(hl.knuckles.clone().applyQuaternion(handQ));
    const shoulder = wpos(B.LeftArm); const elbowPole = shoulder.clone().add(V(0.25, -0.4, -0.2));
    for (let i = 0; i < 2; i++) { twoBone(B.LeftArm, B.LeftForeArm, B.LeftHand, wrist, elbowPole); setWorldQuat(B.LeftHand, handQ); }
    const palmNow = palmLocal[L].clone().applyQuaternion(wquat(B.LeftHand));
    let err = 0;
    for (let n = 1; n <= 4; n++) {
      // a finger that is not pressing a string relaxes into a loose curl beside the others (no target)
      if (!t[n].pressed) { curlFinger(chain(L, names[n]), restOf(L, names[n]), palmNow, n === 4 ? 0.75 : 0.6, 1.25); continue; }
      const tgt = toWorld(t[n].p); const j = solveFinger(chain(L, names[n]), restOf(L, names[n]), tgt, palmNow, t[n].barre ? { maxCurl: 0.22, coupling: 0.4 } : { maxCurl: 1.85 });  // a barre finger is nearly straight, with a slight natural bend
      const tip = wpos(B[`LeftHand${names[n]}4`]); const e = tip.distanceToSquared(tgt); err += e;
      // a fretting finger arches: middle joint ~65–100°, knuckle bent forward; straight 'sticks' are penalised
      // a fretting finger arches over the board and lands on its tip: middle joint ~80–100°, knuckle bent forward
      if (t[n].pressed && !t[n].barre) err += 2.5e-4 * (Math.pow(Math.max(0, 1.45 - j.bend), 2) + 0.5 * Math.pow(Math.max(0, 0.5 - j.flex), 2) + 0.4 * j.spread * j.spread);
      if (t[n].barre) { // the whole first finger must lie across the strings: penalise the middle of the finger lifting off
        const mid = wpos(B.LeftHandIndex2); const want = toWorld(V(t[n].p.x, stringY(Math.min(5, t[n].s + 2.5), t[n].p.x), GUITAR.fretTop + 0.0075)); err += 0.5 * mid.distanceToSquared(want);
      }
    }
    solveFinger(chain(L, 'Thumb'), restOf(L, 'Thumb'), toWorld(t.thumb), palmNow, { coupling: 0.5, maxCurl: 1.0 });
    // keep the knuckles close under the treble edge, as a real player does
    // knuckles sit at the treble edge, level with the board or slightly in front of it, as a real player's do
    err += 0.12 * Math.pow(Math.max(0, hp.ky - 0.012), 2) + 0.06 * Math.pow(Math.max(0, 0.004 - hp.kz), 2);
    return err;
  }
  /* Find the hand placement that lets every pressed fingertip reach its fret (coordinate descent, ~60 solves). */
  const placementCache = new Map(); let lastPlacement = null;
  function optimiseLeft(t) {
    const key = [1, 2, 3, 4].map(n => `${t[n].pressed ? 1 : 0}:${t[n].p.x.toFixed(4)}:${t[n].p.y.toFixed(4)}:${t[n].barre ? 1 : 0}`).join('|');
    if (placementCache.has(key)) return placementCache.get(key);
    let hp = { ky: 0.008, kz: 0.012, pitch: 0, slide: 0 }; let best = solveLeft(t, hp);
    const steps = { ky: 0.012, kz: 0.01, pitch: 0.25, slide: 0.012 }; const lim = { ky: [-0.006, 0.03], kz: [-0.006, 0.032], pitch: [-1.0, 1.0], slide: [-0.035, 0.035] };
    for (let round = 0; round < 7; round++) {
      for (const k of Object.keys(steps)) for (const dir of [1, -1]) {
        const trial = { ...hp, [k]: clamp(hp[k] + dir * steps[k], lim[k][0], lim[k][1]) }; const e = solveLeft(t, trial);
        if (e < best) { best = e; hp = trial; }
      }
      for (const k of Object.keys(steps)) steps[k] *= 0.55;
    }
    lastPlacement = { ...hp, err: best }; placementCache.set(key, hp); if (placementCache.size > 200) placementCache.delete(placementCache.keys().next().value);
    return hp;
  }

  /* --- strumming: just the pick, following the timeline's strokes --- */
  const strum = { x: 0.535, restY: -0.002 };   // strum zone between the sound hole and the bridge (guitar local)
  const pickShape = new THREE.Shape(); pickShape.moveTo(0, 0.016); pickShape.quadraticCurveTo(0.0125, 0.014, 0.011, 0.002); pickShape.quadraticCurveTo(0.004, -0.009, 0, -0.012); pickShape.quadraticCurveTo(-0.004, -0.009, -0.011, 0.002); pickShape.quadraticCurveTo(-0.0125, 0.014, 0, 0.016);
  const pick = new THREE.Mesh(new THREE.ExtrudeGeometry(pickShape, { depth: 0.0009, bevelEnabled: true, bevelThickness: 0.0002, bevelSize: 0.0004, bevelSegments: 2 }), new THREE.MeshStandardMaterial({ color: 0xff7a1a, roughness: 0.32, emissive: 0x3a1400 }));
  pick.geometry.translate(0, 0, -0.00045); pick.castShadow = true; scene.add(pick);
  // tip points into the strings and slightly towards the bridge; the flat face leans towards the viewer so its shape reads
  let lastPickTarget = V();
  function placePick(pickYLocal, pickZOffset) {
    const tipLocal = V(strum.x, pickYLocal, stringZ(strum.x) + 0.001 + pickZOffset);
    lastPickTarget = toWorld(tipLocal);
    // local frame: shape Y axis = from tip back to the body of the pick; shape Z = face normal
    const back = V(-0.66, 0, 0.75).normalize(); const faceN = V(0.75, 0, 0.66).normalize(); const side = new THREE.Vector3().crossVectors(back, faceN);
    const m = new THREE.Matrix4().makeBasis(side, back, faceN); const qLocal = new THREE.Quaternion().setFromRotationMatrix(m);
    pick.quaternion.copy(G.quaternion).multiply(qLocal);
    pick.position.copy(lastPickTarget).sub(V(0, -0.012, 0).applyQuaternion(pick.quaternion));
    pick.updateMatrixWorld(true);
  }

  let talk = 0, talkTarget = 0;
  let currentAnchor = 3;

  /* --- cameras ---
     Audience view of the instrument, kept level: bass strings at the top, headstock on the right.
     'fretting' frames a few frets around the hand (with a strumming inset); 'guitar' shows the whole instrument. */
  const camera = new THREE.PerspectiveCamera(30, 16 / 9, 0.01, 30);
  const hfov = (vfovDeg, aspect) => 2 * Math.atan(Math.tan(THREE.MathUtils.degToRad(vfovDeg) / 2) * aspect);
  function frame3(xFrom, xTo, opts = {}) {
    const fov = opts.fov || 30; const width = xTo - xFrom; const d = (width / 2) / Math.tan(hfov(fov, 16 / 9) / 2);
    const centre = V((xFrom + xTo) / 2, opts.y ?? 0, 0);
    const offset = V(opts.ox ?? 0, -(opts.bass ?? 0.32) * d, d);    // a little from the bass side so fingertips and strings read clearly
    return { pos: toWorld(centre.clone().add(offset)), look: toWorld(centre), up: dirWorld(V(0, -1, 0)), fov };
  }
  const CAMS = {
    // constant real-world width (~23 cm) around the hand, so the hand is the same size wherever it is on the neck
    fretting: () => { const x0 = Math.max(-0.045, fretX(Math.max(0, currentAnchor - 1)) - 0.06); return frame3(x0, x0 + 0.235, { y: 0.012, bass: 0.42 }); },
    guitar: () => frame3(-0.21, 0.88, { bass: 0.12, y: 0.0 }),
    strumming: () => frame3(strum.x - 0.13, strum.x + 0.09, { bass: 0.35, ox: 0.05 }),
  };
  const PIP = { w: 0.26, margin: 12, top: 40 };
  let camName = 'fretting', camFrom = null, camT = 1; const camNow = { pos: V(), look: V(), up: V(0, 1, 0), fov: 30 };
  function setCamera(name, immediate) { if (name === 'wide') name = 'guitar'; if (!CAMS[name]) return; camFrom = immediate ? null : { pos: camNow.pos.clone(), look: camNow.look.clone(), up: camNow.up.clone(), fov: camNow.fov }; camName = name; camT = immediate ? 1 : 0; pipFrame.style.display = name === 'fretting' ? 'block' : 'none'; }
  const pipCam = new THREE.PerspectiveCamera(30, 16 / 9, 0.01, 30);
  const pipFrame = document.createElement('div'); pipFrame.className = 'pip-frame'; pipFrame.innerHTML = '<span>STRUMMING</span>'; container.append(pipFrame);

  /* --- performance driving --- */
  let perf = null, perfTime = -1, capo = 0, playing = false, clockFn = null;
  const gestureKeys = []; // pick y over time
  function buildPickPath(p) {
    gestureKeys.length = 0; if (!p) return;
    for (const g of p.gestures) {
      const yFrom = stringY(g.from, strum.x), yTo = stringY(g.to, strum.x);
      const out = g.stroke === 'U' ? 0.007 : -0.007;   // overshoot beyond the first string of the stroke
      if (g.kind === 'strum') { const n = Math.abs(g.to - g.from); const strokeEnd = g.t + (g.dur - 0.03); // last string sounds at t + (n strings - 1) × stagger
        gestureKeys.push({ t: g.t - 0.07, y: yFrom + (g.stroke === 'D' ? -0.009 : 0.009), z: 0.004 }, { t: g.t, y: yFrom, z: 0, linear: true }, { t: n ? strokeEnd : g.t + 0.02, y: yTo, z: 0 }, { t: strokeEnd + 0.06, y: yTo + (g.stroke === 'D' ? 0.008 : -0.008), z: 0.004 }); }
      else { const y = yFrom; const dir = g.stroke === 'U' ? -1 : 1; gestureKeys.push({ t: g.t - 0.06, y: y - dir * 0.005, z: 0.003 }, { t: g.t, y, z: 0 }, { t: g.t + 0.05, y: y + dir * 0.004, z: 0.003 }); }
    }
    gestureKeys.sort((a, b) => a.t - b.t);
  }
  function pickAt(t) {
    if (!gestureKeys.length || t < gestureKeys[0].t - 0.3) return { y: strum.restY, z: 0.02 };
    let i = 0; while (i < gestureKeys.length - 1 && gestureKeys[i + 1].t <= t) i++;
    const a = gestureKeys[i], b = gestureKeys[i + 1]; if (!b) return { y: a.y, z: a.z };
    const r = clamp((t - a.t) / Math.max(1e-3, b.t - a.t), 0, 1); const k = a.linear ? r : smooth(r); return { y: a.y + (b.y - a.y) * k, z: a.z + (b.z - a.z) * k };
  }

  function setShape(shape, immediate) {
    const next = leftHandTargets(shape, capo); leftShape = shape; currentAnchor = next.anchor;
    if (!leftTargets || immediate) { leftTargets = next; leftFrom = null; return; }
    leftFrom = leftTargets; leftTargets = next; leftStart = clockNow();
    const pressed = shape ? shape.frets.map((f) => f > 0 ? f + capo : (capo > 0 ? capo : 0)) : null; guitarPressed = pressed;
  }
  let guitarPressed = null;
  function currentLeft(now) {
    if (!leftFrom) return leftTargets;
    const k = smooth((now - leftStart) / TWEEN); if (k >= 1) { leftFrom = null; return leftTargets; }
    const out = { anchor: leftTargets.anchor, thumb: leftFrom.thumb.clone().lerp(leftTargets.thumb, k) };
    for (let n = 1; n <= 4; n++) { const a = leftFrom[n], b = leftTargets[n]; const moved = a.p.distanceTo(b.p) > 0.002; const p = a.p.clone().lerp(b.p, k); if (moved) p.z += Math.sin(Math.PI * k) * 0.012; out[n] = { ...b, p }; }
    return out;
  }
  /* --- what the fingers are doing, made obvious ---
     Each pressed string glows in its finger's colour from the fret to the bridge (the part that sounds); open strings that
     are played glow white; muted strings get an × at the nut. Pressed fingertips carry their finger number. */
  const overlay = new THREE.Group(); G.add(overlay);
  const glowMats = {}; const glowMat = (col, op) => (glowMats[col + op] ||= new THREE.MeshBasicMaterial({ color: col, transparent: true, opacity: op, depthWrite: false }));
  const stringGlows = [];
  for (let s = 0; s < 6; s++) {
    const core = new THREE.Mesh(new THREE.CylinderGeometry(0.0011, 0.0011, 1, 8, 1, true), glowMat('#ffffff', 0.9));
    const halo = new THREE.Mesh(new THREE.CylinderGeometry(0.0026, 0.0026, 1, 10, 1, true), glowMat('#ffffff', 0.22));
    core.visible = halo.visible = false; core.renderOrder = halo.renderOrder = 5; overlay.add(core, halo); stringGlows.push({ core, halo });
  }
  function placeSegment(mesh, a, b) { const mid = a.clone().add(b).multiplyScalar(0.5); mesh.position.copy(mid); mesh.scale.set(1, a.distanceTo(b), 1); mesh.quaternion.setFromUnitVectors(V(0, 1, 0), b.clone().sub(a).normalize()); }
  const stringNames = ['E', 'A', 'D', 'G', 'B', 'e'];
  const nutLabels = stringNames.map((n, s) => { const sp = labelSprite(n, { fg: '#e9f2e8', size: 0.0072 }); sp.position.set(-0.016, stringY(s, 0), stringZ(0) + 0.002); overlay.add(sp); return sp; });
  const nutMarks = stringNames.map((n, s) => { const sp = labelSprite('×', { fg: '#ff8a80', size: 0.0072 }); sp.position.set(-0.03, stringY(s, 0), stringZ(0) + 0.002); sp.visible = false; overlay.add(sp); return sp; });
  const fretNums = []; for (let n = 1; n <= 15; n++) { const x = (fretX(n) + fretX(n - 1)) / 2; const sp = labelSprite(String(n), { fg: '#c9d6c6', size: 0.0062, bold: false }); sp.position.set(x, boardWidth(x) / 2 + 0.0085, GUITAR.boardTop); overlay.add(sp); fretNums.push(sp); }
  const fingerBadges = {}; for (let n = 1; n <= 4; n++) { const sp = labelSprite(String(n), { bg: FINGER_COLOURS[n], fg: '#0d1410', size: 0.0092 }); sp.visible = false; scene.add(sp); fingerBadges[n] = sp; }
  let overlaysOn = true;
  function updateOverlay(now) {
    const shape = leftShape; const frets = shape ? shape.frets : null; const fingers = shape ? shape.fingers : null;
    for (let s = 0; s < 6; s++) {
      const gl = stringGlows[s]; const f = frets ? frets[s] : -1; const show = overlaysOn && frets && f >= 0;
      gl.core.visible = gl.halo.visible = !!show; nutMarks[s].visible = overlaysOn && !!frets && f < 0 && frets.some(x => x >= 0);
      if (!show) continue;
      const phys = f > 0 ? f + capo : capo; const x0 = phys > 0 ? fretX(phys) : 0; const z0 = phys > 0 ? GUITAR.fretTop + 0.0004 : stringZ(0);
      placeSegment(gl.core, V(x0, stringY(s, x0), z0), V(GUITAR.scale, stringY(s, GUITAR.scale), stringZ(GUITAR.scale)));
      placeSegment(gl.halo, V(x0, stringY(s, x0), z0), V(GUITAR.scale, stringY(s, GUITAR.scale), stringZ(GUITAR.scale)));
      const col = f > 0 ? FINGER_COLOURS[fingers[s] || 1] : FINGER_COLOURS[0]; const amp = guitar.strings[s].amp;
      gl.core.material = glowMat(col, f > 0 ? 0.92 : 0.55); gl.halo.material = glowMat(col, Math.min(0.6, (f > 0 ? 0.22 : 0.12) + amp * 0.35));
    }
    for (let n = 1; n <= 4; n++) {
      const t = leftTargets && leftTargets[n]; const b = fingerBadges[n]; b.visible = overlaysOn && !!t && t.pressed;
      if (!b.visible) continue;
      // badge sits just above the fingertip, towards the viewer
      // the badge marks the exact spot where this finger presses its string (pulled along the view ray so it sits on top)
      const contact = t.barre ? toWorld(V(t.p.x, stringY(Math.max(0, t.s) + 1.5, t.p.x), stringZ(t.p.x))) : toWorld(V(t.p.x, t.p.y, stringZ(t.p.x)));
      b.position.copy(contact).add(camera.position.clone().sub(contact).normalize().multiplyScalar(0.012));
    }
    nutLabels.forEach(l => l.visible = overlaysOn); fretNums.forEach((l, i) => l.visible = overlaysOn && i + 1 > capo);
  }

  const t0 = performance.now(); const clockNow = () => (performance.now() - t0) / 1000;

  function pluck(ev) { const st = guitar.strings[ev.string]; if (st) { st.amp = Math.min(1.2, st.amp + 0.9 * (ev.vel / 0.3)); } }

  /* --- frame --- */
  function resize() { const w = container.clientWidth || 640, h = container.clientHeight || 360; renderer.setSize(w, h, false); camera.aspect = w / h; camera.updateProjectionMatrix(); const pw = Math.round(w * PIP.w), ph = Math.round(pw * 9 / 16); Object.assign(pipFrame.style, { width: pw + 'px', height: ph + 'px', left: PIP.margin + 'px', top: PIP.top + 'px' }); }
  const ro = typeof ResizeObserver !== 'undefined' ? new ResizeObserver(resize) : null; if (ro) ro.observe(container); applyQuality(); resize();
  let slowFrames = 0, lastFrameAt = 0;
  let raf = null, lastT = clockNow(); const onFrameHooks = [];
  const frameStats = { n: 0, solveMs: 0, renderMs: 0 };
  function aimCamera(cam, view, aspect) {
    // keep the same horizontal framing on narrower screens (phones) by widening the vertical field of view
    let vfov = view.fov; if (aspect < 16 / 9) { const h = hfov(vfov, 16 / 9); vfov = Math.min(75, THREE.MathUtils.radToDeg(2 * Math.atan(Math.tan(h / 2) / aspect))); }
    cam.position.copy(view.pos); cam.up.copy(view.up); cam.lookAt(view.look); if (Math.abs(cam.fov - vfov) > 0.01 || cam.aspect !== aspect) { cam.fov = vfov; cam.aspect = aspect; cam.updateProjectionMatrix(); }
  }
  let lightsPlaced = false;
  function frame() {
    const f0 = performance.now();
    if (lastFrameAt && quality === 'high') { slowFrames = f0 - lastFrameAt > 60 ? slowFrames + 1 : Math.max(0, slowFrames - 1); if (slowFrames > 40) { quality = 'low'; applyQuality(); } } lastFrameAt = f0;
    const now = clockNow(); const dt = Math.min(0.05, now - lastT); lastT = now;
    placeGuitar();
    if (!lightsPlaced) { // key light from in front of the board, a little from the bass side and the headstock, so fingers cast soft shadows onto the fretboard
      const c = toWorld(V(0.3, 0, 0)); key.target.position.copy(c); key.position.copy(c).add(dirWorld(V(-0.25, -0.45, 1)).multiplyScalar(2));
      fill.target.position.copy(c); fill.position.copy(c).add(dirWorld(V(0.4, 0.5, 0.8)).multiplyScalar(2)); lightsPlaced = true;
    }
    const lt = currentLeft(now); const hpT = optimiseLeft(leftTargets); let hp = hpT;
    if (leftFrom) { const hpF = optimiseLeft(leftFrom); const k = smooth((now - leftStart) / TWEEN); hp = { ky: hpF.ky + (hpT.ky - hpF.ky) * k, kz: hpF.kz + (hpT.kz - hpF.kz) * k, pitch: hpF.pitch + (hpT.pitch - hpF.pitch) * k, slide: hpF.slide + (hpT.slide - hpF.slide) * k }; }
    solveLeft(lt, hp);
    if (clockFn) { const ct = clockFn(); playing = ct !== null && ct !== undefined; perfTime = playing ? ct : -1; }
    const pt = playing && perfTime >= 0 ? pickAt(perfTime) : { y: strum.restY, z: 0.02 + Math.sin(now * 1.3) * 0.002 };
    placePick(pt.y, pt.z);
    talk += (talkTarget - talk) * Math.min(1, dt * 8);
    // camera
    camT = Math.min(1, camT + dt * 1.6); const want = CAMS[camName]();
    if (camFrom && camT < 1) { const k = smooth(camT); camNow.pos.copy(camFrom.pos).lerp(want.pos, k); camNow.look.copy(camFrom.look).lerp(want.look, k); camNow.up.copy(camFrom.up).lerp(want.up, k).normalize(); camNow.fov = camFrom.fov + (want.fov - camFrom.fov) * k; }
    else { camNow.pos.copy(want.pos); camNow.look.copy(want.look); camNow.up.copy(want.up); camNow.fov = want.fov; }
    aimCamera(camera, camNow, camera.aspect);
    // strings vibrate and bend to the frets they are pressed at
    for (const st of guitar.strings) { st.phase += dt * st.freq * 2 * Math.PI; st.amp *= Math.exp(-dt * 2.4); if (st.amp < 0.002) st.amp = 0; }
    guitar.layoutStrings(leftShape ? leftShape.frets.map(f => f > 0 ? f + capo : 0) : null);
    updateOverlay(now);
    for (const h of onFrameHooks) h(now);
    const f1 = performance.now();
    const size = renderer.getSize(new THREE.Vector2()); renderer.setScissorTest(false); renderer.setViewport(0, 0, size.x, size.y); renderer.render(scene, camera);
    if (camName === 'fretting') { // strumming inset, top left
      const pw = Math.round(size.x * PIP.w), ph = Math.round(pw * 9 / 16), px = PIP.margin, py = size.y - PIP.top - ph;
      aimCamera(pipCam, CAMS.strumming(), 16 / 9); renderer.setScissorTest(true); renderer.setScissor(px, py, pw, ph); renderer.setViewport(px, py, pw, ph); renderer.render(scene, pipCam); renderer.setScissorTest(false); renderer.setViewport(0, 0, size.x, size.y);
    }
    const f2 = performance.now();
    frameStats.n++; frameStats.solveMs = frameStats.solveMs * 0.9 + (f1 - f0) * 0.1; frameStats.renderMs = frameStats.renderMs * 0.9 + (f2 - f1) * 0.1;
    raf = requestAnimationFrame(frame);
  }
  placeGuitar(); setShape(null, true); { const v = CAMS.fretting(); camNow.pos.copy(v.pos); camNow.look.copy(v.look); camNow.up.copy(v.up); camNow.fov = v.fov; } setCamera('fretting', true);
  frame();

  return {
    setShape, pluck, setCamera, cameras: Object.keys(CAMS),
    setCapo(n) { capo = n; guitar.setCapo(n); if (leftShape) setShape(leftShape, true); },
    setPerformance(p) { perf = p; buildPickPath(p); },
    setTime(t, isPlaying) { perfTime = t; playing = isPlaying; },
    setClock(fn) { clockFn = fn; },
    setTalking(on) { talkTarget = on ? 1 : 0; },
    look() { },
    nod() { },
    portrait() { return Promise.resolve(null); },
    setOverlays(on) { overlaysOn = !!on; },
    overlaysOn: () => overlaysOn,
    cameraName: () => camName,
    stringGlow: () => stringGlows.map(g => g.core.visible ? '#' + g.core.material.color.getHexString() : null),
    badges: () => Object.fromEntries(Object.entries(fingerBadges).map(([n, b]) => [n, b.visible])),
    handMesh,
    showHints(on) { if (!on || !leftShape) { guitar.showHints(null); return; } const pts = []; leftShape.frets.forEach((f, s) => { if (f > 0) pts.push(pressPoint(s, f + capo)); }); guitar.showHints(pts); },
    /* for tests: world-space distance from each fingertip to where it should press */
    fingertipErrors() {
      const out = {}; const names = { 1: 'Index', 2: 'Middle', 3: 'Ring', 4: 'Pinky' }; if (!leftTargets) return out;
      for (let n = 1; n <= 4; n++) { const t = leftTargets[n]; if (!t.pressed || t.barre) continue; const tip = wpos(B[`LeftHand${names[n]}4`]); out[n] = { string: t.s, fret: t.phys, mm: +(tip.distanceTo(toWorld(t.p)) * 1000).toFixed(1) }; }
      return out;
    },
    pickPathY: (t) => pickAt(t).y,
    fingerTargets: () => { const o = {}; if (!leftTargets) return o; for (let n = 1; n <= 4; n++) { const t = leftTargets[n]; o[n] = { pressed: !!t.pressed, barre: !!t.barre, string: t.s, phys: t.phys }; } return o; },
    frameStats: () => ({ ...frameStats, quality }),
    handReport: () => handReport,
    placement: () => lastPlacement,
    pickWorld: () => pick.localToWorld(V(0, -0.012, 0)),
    pickError: () => { if (!pick) return null; const tip = pick.localToWorld(V(0, -0.012, 0)); return +(tip.distanceTo(lastPickTarget) * 1000).toFixed(1); },
    stringWorld: (s, x = strum.x) => toWorld(V(x, stringY(s, x), stringZ(x))),
    renderer, scene, camera, bones: B, guitar, onFrame: (fn) => onFrameHooks.push(fn),
    dispose() { cancelAnimationFrame(raf); if (ro) ro.disconnect(); renderer.dispose(); },
  };
}
