// hero-gl.ts — WebGL2 fragment shader for hero dot-grid landscape animation.
// Replaces Canvas 2D worker: all dot computation runs on the GPU in a single draw call.

const VERT = /* glsl */ `#version 300 es
// Fullscreen triangle from gl_VertexID — no vertex buffers needed
void main() {
  float x = float((gl_VertexID & 1) << 2) - 1.0;
  float y = float((gl_VertexID & 2) << 1) - 1.0;
  gl_Position = vec4(x, y, 0.0, 1.0);
}`;

const FRAG = /* glsl */ `#version 300 es
precision mediump float;

uniform float uTime;
uniform vec2  uResolution;
uniform vec2  uMouse;
uniform float uMouseActive;

out vec4 fragColor;

// Deterministic per-cell random
float hash(vec2 p) {
  return fract(sin(dot(p, vec2(12.9898, 78.233))) * 43758.5453);
}

// Smoothstep helper
float ssmooth(float a, float b, float t) {
  float x = clamp((t - a) / (b - a), 0.0, 1.0);
  return x * x * (3.0 - 2.0 * x);
}

void main() {
  // Grid parameters (match Canvas 2D: 10px spacing, 2px dot radius)
  float spacing = 10.0;
  float dotRadius = 2.0;

  vec2 uv = gl_FragCoord.xy / uResolution;
  // Flip Y: shader Y=0 is bottom, we want top-down like Canvas
  uv.y = 1.0 - uv.y;
  vec2 pos = uv * uResolution;

  // Grid cell
  vec2 cell = floor(pos / spacing);
  vec2 cellCenter = (cell + 0.5) * spacing;
  float dist = length(pos - cellCenter);

  // Discard pixels outside dot radius
  if (dist > dotRadius) discard;

  // Normalized row position (0=top, 1=bottom)
  float totalCols = ceil(uResolution.x / spacing);
  float totalRows = ceil(uResolution.y / spacing);
  float ny = cell.y / totalRows;
  float nx = cell.x / totalCols;

  // Three-layer sine ridgeline
  float ridge = 0.72
    + sin(nx * 3.14159 * 2.3 + 0.8) * 0.06
    + sin(nx * 3.14159 * 5.1 + 2.1) * 0.03
    + sin(nx * 3.14159 * 11.7 + 4.3) * 0.015;

  float cloudTop = ridge - 0.12;

  // Smooth sky/mountain blend
  float fadeBand = 0.06;
  float blendCenter = (cloudTop + ridge) * 0.5;
  float blendHalf = max((ridge - cloudTop) * 0.5 + fadeBand, 0.01);
  float blend = ssmooth(blendCenter - blendHalf, blendCenter + blendHalf, ny);

  // Sky color: warm peach gradient
  float skyT = min(ny / max(cloudTop, 0.01), 1.0);
  vec3 skyCol = mix(vec3(248.0, 220.0, 195.0), vec3(240.0, 195.0, 170.0), skyT) / 255.0;

  // Mountain color: peach-orange to burnt orange
  float mtnDepth = clamp((ny - ridge) / max(1.0 - ridge, 0.01), 0.0, 1.0);
  vec3 mtnCol = mix(vec3(238.0, 150.0, 90.0), vec3(200.0, 105.0, 55.0), mtnDepth) / 255.0;

  vec3 color = mix(skyCol, mtnCol, blend);

  // Per-cell random phase
  float phase = hash(cell) * 6.28318;

  // Base opacity: sky nearly invisible, mountains bold
  float skyBase = 0.02 + hash(cell + 100.0) * 0.02;
  float mtnBase = 0.25 + hash(cell + 200.0) * 0.10;
  float base = mix(skyBase, mtnBase, blend);

  // Center elliptical suppression for text readability
  vec2 center = uResolution * 0.5;
  float minDim = min(center.x, center.y);
  float cdist = length((pos - center) * vec2(1.0, 1.4));
  float innerR = minDim * 0.2;
  float outerR = minDim * 0.55;
  if (cdist < outerR) {
    float t = max(0.0, (cdist - innerR) / (outerR - innerR));
    base *= 0.45 + 0.55 * t * t;
  }

  // Ambient breathing
  float breath = sin(uTime * 0.6 + phase) * 0.03;
  float opacity = base + breath;

  // Attractor 1 (Lissajous, computed in shader)
  vec2 at1 = vec2(
    uResolution.x * 0.5 + sin(uTime * 0.25) * uResolution.x * 0.42,
    uResolution.y * 0.5 + cos(uTime * 0.18) * uResolution.y * 0.38
  );
  float at1Dist = length(cellCenter - at1);
  float at1R = 260.0;
  if (at1Dist < at1R) {
    float ratio = 1.0 - at1Dist / at1R;
    float ease = ratio * ratio;
    opacity = opacity + (0.28 - opacity) * ease;
  }

  // Attractor 2
  vec2 at2 = vec2(
    uResolution.x * 0.5 + cos(uTime * 0.15) * uResolution.x * 0.35,
    uResolution.y * 0.5 + sin(uTime * 0.22) * uResolution.y * 0.32
  );
  float at2R = 260.0 * 0.8;
  float at2Dist = length(cellCenter - at2);
  if (at2Dist < at2R) {
    float ratio = 1.0 - at2Dist / at2R;
    float ease = ratio * ratio;
    opacity = opacity + (0.22 - opacity) * ease * 0.6;
  }

  // Cursor interaction
  if (uMouseActive > 0.5) {
    float cursorR = 220.0;
    float mDist = length(cellCenter - uMouse);
    if (mDist < cursorR) {
      float ratio = 1.0 - mDist / cursorR;
      float ease = ratio * ratio;
      opacity = opacity + (0.35 - opacity) * ease;
    }
  }

  opacity = clamp(opacity, 0.02, 0.35);

  fragColor = vec4(color, opacity);
}`;

export interface HeroGLController {
  setMouse(x: number, y: number, active: boolean): void;
  setVisible(visible: boolean): void;
  resize(): void;
  destroy(): void;
}

function compileShader(gl: WebGL2RenderingContext, type: number, src: string): WebGLShader | null {
  const s = gl.createShader(type);
  if (!s) return null;
  gl.shaderSource(s, src);
  gl.compileShader(s);
  if (!gl.getShaderParameter(s, gl.COMPILE_STATUS)) {
    console.error('Shader compile error:', gl.getShaderInfoLog(s));
    gl.deleteShader(s);
    return null;
  }
  return s;
}

function createProgram(gl: WebGL2RenderingContext): WebGLProgram | null {
  const vs = compileShader(gl, gl.VERTEX_SHADER, VERT);
  const fs = compileShader(gl, gl.FRAGMENT_SHADER, FRAG);
  if (!vs || !fs) return null;
  const prog = gl.createProgram()!;
  gl.attachShader(prog, vs);
  gl.attachShader(prog, fs);
  gl.linkProgram(prog);
  if (!gl.getProgramParameter(prog, gl.LINK_STATUS)) {
    console.error('Program link error:', gl.getProgramInfoLog(prog));
    gl.deleteProgram(prog);
    return null;
  }
  // Shaders can be detached after linking
  gl.detachShader(prog, vs);
  gl.detachShader(prog, fs);
  gl.deleteShader(vs);
  gl.deleteShader(fs);
  return prog;
}

export function initHeroGL(canvas: HTMLCanvasElement): HeroGLController | null {
  const gl = canvas.getContext('webgl2', { alpha: true, premultipliedAlpha: false, antialias: false });
  if (!gl) return null;

  let program = createProgram(gl);
  if (!program) return null;

  const DPR = Math.min(window.devicePixelRatio || 1, 2);
  let reducedMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  // Uniform locations
  let uTime: WebGLUniformLocation | null;
  let uResolution: WebGLUniformLocation | null;
  let uMouse: WebGLUniformLocation | null;
  let uMouseActive: WebGLUniformLocation | null;

  function cacheLocations() {
    uTime = gl!.getUniformLocation(program!, 'uTime');
    uResolution = gl!.getUniformLocation(program!, 'uResolution');
    uMouse = gl!.getUniformLocation(program!, 'uMouse');
    uMouseActive = gl!.getUniformLocation(program!, 'uMouseActive');
  }
  cacheLocations();

  // Empty VAO required by WebGL2
  let vao = gl.createVertexArray();
  gl.bindVertexArray(vao);

  // Blending for alpha
  gl.enable(gl.BLEND);
  gl.blendFunc(gl.SRC_ALPHA, gl.ONE_MINUS_SRC_ALPHA);

  // State
  let mouseX = -9999;
  let mouseY = -9999;
  let mouseActiveVal = 0.0;
  let animId = 0;
  let visible = true;
  let destroyed = false;

  function sizeCanvas() {
    if (!canvas.parentElement) return;
    const rect = canvas.parentElement.getBoundingClientRect();
    const pw = rect.width * DPR;
    const ph = rect.height * DPR;
    canvas.width = pw;
    canvas.height = ph;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = rect.height + 'px';
    gl!.viewport(0, 0, pw, ph);
  }

  function render(time: number) {
    if (destroyed) return;
    gl!.useProgram(program);
    // Pass logical resolution (physical / DPR) so shader spacing matches CSS pixels
    gl!.uniform1f(uTime, time * 0.001);
    gl!.uniform2f(uResolution, canvas.width / DPR, canvas.height / DPR);
    gl!.uniform2f(uMouse, mouseX, mouseY);
    gl!.uniform1f(uMouseActive, mouseActiveVal);

    gl!.clearColor(0, 0, 0, 0);
    gl!.clear(gl!.COLOR_BUFFER_BIT);
    gl!.drawArrays(gl!.TRIANGLES, 0, 3);

    if (visible && !reducedMotion) {
      animId = requestAnimationFrame(render);
    } else {
      animId = 0;
    }
  }

  // Context loss/restore
  canvas.addEventListener('webglcontextlost', (e) => {
    e.preventDefault();
    cancelAnimationFrame(animId);
    animId = 0;
  });

  canvas.addEventListener('webglcontextrestored', () => {
    program = createProgram(gl!);
    if (!program) return;
    cacheLocations();
    vao = gl!.createVertexArray();
    gl!.bindVertexArray(vao);
    gl!.enable(gl!.BLEND);
    gl!.blendFunc(gl!.SRC_ALPHA, gl!.ONE_MINUS_SRC_ALPHA);
    sizeCanvas();
    if (visible && !reducedMotion) {
      animId = requestAnimationFrame(render);
    } else {
      render(0);
    }
  });

  // Initial size + first frame
  sizeCanvas();
  if (reducedMotion) {
    render(0);
  } else {
    animId = requestAnimationFrame(render);
  }

  // Listen for reduced-motion preference changes
  window.matchMedia('(prefers-reduced-motion: reduce)').addEventListener('change', (e) => {
    reducedMotion = e.matches;
    if (e.matches) {
      cancelAnimationFrame(animId);
      animId = 0;
      render(0);
    } else if (visible && animId === 0) {
      animId = requestAnimationFrame(render);
    }
  });

  return {
    setMouse(x: number, y: number, active: boolean) {
      mouseX = x;
      mouseY = y;
      mouseActiveVal = active ? 1.0 : 0.0;
    },
    setVisible(v: boolean) {
      visible = v;
      if (v && !reducedMotion && animId === 0) {
        animId = requestAnimationFrame(render);
      } else if (!v) {
        cancelAnimationFrame(animId);
        animId = 0;
      }
    },
    resize() {
      sizeCanvas();
      if (reducedMotion) render(0);
    },
    destroy() {
      destroyed = true;
      cancelAnimationFrame(animId);
      animId = 0;
      gl!.deleteVertexArray(vao);
      if (program) gl!.deleteProgram(program);
    },
  };
}
