// hero-worker.ts — OffscreenCanvas Web Worker for hero grid animation
// Runs the entire grid build + animate loop off the main thread.

interface InitMessage {
  type: 'init';
  canvas: OffscreenCanvas;
  dpr: number;
  w: number;
  h: number;
  reducedMotion: boolean;
}

interface MouseMessage {
  type: 'mouse';
  x: number;
  y: number;
  active: boolean;
}

interface ResizeMessage {
  type: 'resize';
  w: number;
  h: number;
  dpr: number;
}

interface ReducedMotionMessage {
  type: 'reducedMotion';
  value: boolean;
}

interface VisibilityMessage {
  type: 'visibility';
  visible: boolean;
}

type WorkerMessage = InitMessage | MouseMessage | ResizeMessage | ReducedMotionMessage | VisibilityMessage;

// ─── Inlined from canvas-utils.ts (workers can't import from main bundle) ───
function getAttractorPositions(t: number, w: number, h: number) {
  return {
    at1X: w * 0.5 + Math.sin(t * 0.25) * w * 0.42,
    at1Y: h * 0.5 + Math.cos(t * 0.18) * h * 0.38,
    at2X: w * 0.5 + Math.cos(t * 0.15) * w * 0.35,
    at2Y: h * 0.5 + Math.sin(t * 0.22) * h * 0.32,
  };
}

// ─── Constants ───
const GRID_SPACING = 10;
const DOT_RADIUS = 2;
const CURSOR_RADIUS = 220;
const ATTRACTOR_RADIUS = 260;

// Landscape tuning
const RIDGE_BASE = 0.72;
const RIDGE_AMP = [0.06, 0.03, 0.015];
const CLOUD_BAND = 0.12;
const CENTER_SUPPRESS = 0.45;

type Zone = 'sky' | 'cloud' | 'mountain';

interface CharCell {
  x: number;
  y: number;
  phase: number;
  baseOpacity: number;
  r: number;
  g: number;
  b: number;
  zone: Zone;
  colorStr: string;
}

// ─── Mutable state ───
let canvas: OffscreenCanvas | null = null;
let ctx: OffscreenCanvasRenderingContext2D | null = null;
let dpr = 1;
let cells: CharCell[] = [];
let mouseX = -9999;
let mouseY = -9999;
let mouseActive = false;
let animId = 0;
let isVisible = true;
let canvasW = 0;
let canvasH = 0;
let reducedMotion = false;

// ─── Grid helpers ───

function ridgelineAt(col: number, totalCols: number): number {
  const nx = col / totalCols;
  return RIDGE_BASE
    + Math.sin(nx * Math.PI * 2.3 + 0.8) * RIDGE_AMP[0]
    + Math.sin(nx * Math.PI * 5.1 + 2.1) * RIDGE_AMP[1]
    + Math.sin(nx * Math.PI * 11.7 + 4.3) * RIDGE_AMP[2];
}

function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t;
}

function buildGrid() {
  cells = [];
  const cols = Math.ceil(canvasW / GRID_SPACING) + 1;
  const rows = Math.ceil(canvasH / GRID_SPACING) + 1;
  const cx = canvasW / 2;
  const cy = canvasH / 2;

  for (let r = 0; r < rows; r++) {
    for (let c = 0; c < cols; c++) {
      const x = c * GRID_SPACING;
      const y = r * GRID_SPACING;
      const ny = r / rows;
      const ridge = ridgelineAt(c, cols);
      const cloudTop = ridge - CLOUD_BAND;

      // Smooth blend: 0 = pure sky, 1 = pure mountain
      const FADE_BAND = 0.06;
      const blendCenter = (cloudTop + ridge) / 2;
      const blendHalf = Math.max((ridge - cloudTop) / 2 + FADE_BAND, 0.01);
      const rawBlend = (ny - blendCenter + blendHalf) / (blendHalf * 2);
      const cb2 = Math.max(0, Math.min(rawBlend, 1));
      const blend = cb2 * cb2 * (3 - 2 * cb2); // smoothstep

      // Sky: warm peach
      const skyT = Math.min(ny / Math.max(cloudTop, 0.01), 1);
      const skyR = lerp(248, 240, skyT);
      const skyG = lerp(220, 195, skyT);
      const skyB = lerp(195, 170, skyT);

      // Mountain: warm peach-orange to burnt orange
      const mtnDepth = Math.min(Math.max(0, (ny - ridge) / Math.max(1 - ridge, 0.01)), 1);
      const mtnR = lerp(238, 200, mtnDepth);
      const mtnG = lerp(150, 105, mtnDepth);
      const mtnB = lerp(90, 55, mtnDepth);

      const cr = Math.round(lerp(skyR, mtnR, blend));
      const cg = Math.round(lerp(skyG, mtnG, blend));
      const cb = Math.round(lerp(skyB, mtnB, blend));

      // High contrast: nearly invisible sky, bold mountains
      const skyBase = 0.02 + Math.random() * 0.02;
      const mtnBase = 0.25 + Math.random() * 0.10;
      let base = lerp(skyBase, mtnBase, blend);

      let zone: Zone;
      if (blend < 0.3) {
        zone = 'sky';
      } else if (blend > 0.7) {
        zone = 'mountain';
      } else {
        zone = 'cloud';
      }

      // Center suppression — smooth radial fade for text readability
      const dist = Math.sqrt((x - cx) ** 2 + ((y - cy) * 1.4) ** 2);
      const innerR = Math.min(cx, cy) * 0.2;
      const outerR = Math.min(cx, cy) * 0.55;
      if (dist < outerR) {
        const t = Math.max(0, (dist - innerR) / (outerR - innerR));
        const fade = t * t;
        base *= CENTER_SUPPRESS + (1 - CENTER_SUPPRESS) * fade;
      }

      cells.push({
        x,
        y,
        phase: Math.random() * Math.PI * 2,
        baseOpacity: base,
        r: cr,
        g: cg,
        b: cb,
        zone,
        colorStr: `rgb(${cr},${cg},${cb})`,
      });
    }
  }
}

function sizeCanvas(w: number, h: number, newDpr: number) {
  if (!canvas || !ctx) return;
  dpr = newDpr;
  canvas.width = w * dpr;
  canvas.height = h * dpr;
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  canvasW = w;
  canvasH = h;
  buildGrid();
}

function drawStatic() {
  if (!ctx) return;
  ctx.clearRect(0, 0, canvasW, canvasH);

  for (const cell of cells) {
    ctx.globalAlpha = cell.baseOpacity;
    ctx.fillStyle = cell.colorStr;
    ctx.fillRect(cell.x - DOT_RADIUS, cell.y - DOT_RADIUS, DOT_RADIUS * 2, DOT_RADIUS * 2);
  }
  ctx.globalAlpha = 1;
}

function animate(time: number) {
  if (!ctx) return;

  ctx.clearRect(0, 0, canvasW, canvasH);

  const t = time * 0.001;

  const { at1X, at1Y, at2X, at2Y } = getAttractorPositions(t, canvasW, canvasH);

  // Pre-compute bounding boxes for early-out checks
  const at1Left = at1X - ATTRACTOR_RADIUS;
  const at1Right = at1X + ATTRACTOR_RADIUS;
  const at1Top = at1Y - ATTRACTOR_RADIUS;
  const at1Bottom = at1Y + ATTRACTOR_RADIUS;

  const a2r = ATTRACTOR_RADIUS * 0.8;
  const at2Left = at2X - a2r;
  const at2Right = at2X + a2r;
  const at2Top = at2Y - a2r;
  const at2Bottom = at2Y + a2r;

  const curLeft = mouseX - CURSOR_RADIUS;
  const curRight = mouseX + CURSOR_RADIUS;
  const curTop = mouseY - CURSOR_RADIUS;
  const curBottom = mouseY + CURSOR_RADIUS;

  for (const cell of cells) {
    // Ambient breathing
    const breath = Math.sin(t * 0.6 + cell.phase) * 0.03;
    let opacity = cell.baseOpacity + breath;

    // Attractor 1 — early-out bounding box check
    if (cell.x >= at1Left && cell.x <= at1Right && cell.y >= at1Top && cell.y <= at1Bottom) {
      const adx = cell.x - at1X;
      const ady = cell.y - at1Y;
      const adistSq = adx * adx + ady * ady;
      if (adistSq < ATTRACTOR_RADIUS * ATTRACTOR_RADIUS) {
        const ratio = 1 - (Math.sqrt(adistSq) / ATTRACTOR_RADIUS);
        const ease = ratio * ratio;
        opacity = opacity + (0.28 - opacity) * ease;
      }
    }

    // Attractor 2 — early-out bounding box check
    if (cell.x >= at2Left && cell.x <= at2Right && cell.y >= at2Top && cell.y <= at2Bottom) {
      const adx = cell.x - at2X;
      const ady = cell.y - at2Y;
      const adistSq = adx * adx + ady * ady;
      if (adistSq < a2r * a2r) {
        const ratio = 1 - (Math.sqrt(adistSq) / a2r);
        const ease = ratio * ratio;
        opacity = opacity + (0.22 - opacity) * ease * 0.6;
      }
    }

    // Cursor interaction — early-out bounding box check
    if (mouseActive && cell.x >= curLeft && cell.x <= curRight && cell.y >= curTop && cell.y <= curBottom) {
      const cdx = cell.x - mouseX;
      const cdy = cell.y - mouseY;
      const cdistSq = cdx * cdx + cdy * cdy;
      if (cdistSq < CURSOR_RADIUS * CURSOR_RADIUS) {
        const ratio = 1 - (Math.sqrt(cdistSq) / CURSOR_RADIUS);
        const ease = ratio * ratio;
        opacity = opacity + (0.35 - opacity) * ease;
      }
    }

    ctx.globalAlpha = Math.max(0.02, Math.min(opacity, 0.35));
    ctx.fillStyle = cell.colorStr;
    ctx.fillRect(cell.x - DOT_RADIUS, cell.y - DOT_RADIUS, DOT_RADIUS * 2, DOT_RADIUS * 2);
  }

  ctx.globalAlpha = 1;
  animId = isVisible ? requestAnimationFrame(animate) : 0;
}

// ─── Message handler ───
self.onmessage = (e: MessageEvent<WorkerMessage>) => {
  const msg = e.data;

  switch (msg.type) {
    case 'init':
      canvas = msg.canvas;
      ctx = canvas.getContext('2d');
      if (!ctx) return;
      reducedMotion = msg.reducedMotion;
      sizeCanvas(msg.w, msg.h, msg.dpr);
      if (reducedMotion) {
        drawStatic();
      } else {
        isVisible = true;
        animId = requestAnimationFrame(animate);
      }
      break;

    case 'mouse':
      mouseX = msg.x;
      mouseY = msg.y;
      mouseActive = msg.active;
      break;

    case 'resize':
      sizeCanvas(msg.w, msg.h, msg.dpr);
      if (reducedMotion) drawStatic();
      break;

    case 'reducedMotion':
      reducedMotion = msg.value;
      if (reducedMotion) {
        cancelAnimationFrame(animId);
        animId = 0;
        drawStatic();
      } else if (isVisible && animId === 0) {
        animId = requestAnimationFrame(animate);
      }
      break;

    case 'visibility':
      isVisible = msg.visible;
      if (isVisible && !reducedMotion && animId === 0) {
        animId = requestAnimationFrame(animate);
      } else if (!isVisible) {
        cancelAnimationFrame(animId);
        animId = 0;
      }
      break;
  }
};
