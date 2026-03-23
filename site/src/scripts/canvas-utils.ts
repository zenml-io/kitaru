/** Size a canvas to fill its parent at the given DPR. Returns logical dimensions. */
export function sizeCanvasToParent(
  canvas: HTMLCanvasElement,
  ctx: CanvasRenderingContext2D,
  dpr: number
): { w: number; h: number } {
  if (!canvas.parentElement) return { w: 0, h: 0 };
  const rect = canvas.parentElement.getBoundingClientRect();
  canvas.width = rect.width * dpr;
  canvas.height = rect.height * dpr;
  canvas.style.width = rect.width + 'px';
  canvas.style.height = rect.height + 'px';
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  return { w: rect.width, h: rect.height };
}

/** Track mouse/touch position relative to a canvas within a section. Returns cleanup function. */
export function attachMouseTracker(
  section: HTMLElement,
  canvas: HTMLCanvasElement,
  onMove: (x: number, y: number) => void,
  onLeave: () => void
): () => void {
  // Cache the rect and invalidate on resize/scroll to avoid per-mousemove layout reflow
  let cachedRect: DOMRect | null = null;

  function getRect(): DOMRect {
    if (!cachedRect) cachedRect = canvas.getBoundingClientRect();
    return cachedRect;
  }

  function invalidateRect() { cachedRect = null; }

  function update(clientX: number, clientY: number) {
    const rect = getRect();
    onMove(clientX - rect.left, clientY - rect.top);
  }

  const handleMouseMove = (e: MouseEvent) => update(e.clientX, e.clientY);
  const handleTouchMove = (e: TouchEvent) => {
    if (e.touches.length > 0) update(e.touches[0].clientX, e.touches[0].clientY);
  };
  const handleLeave = () => onLeave();

  section.addEventListener('mousemove', handleMouseMove);
  section.addEventListener('touchmove', handleTouchMove, { passive: true });
  section.addEventListener('mouseleave', handleLeave);
  section.addEventListener('touchend', handleLeave);
  window.addEventListener('resize', invalidateRect);
  window.addEventListener('scroll', invalidateRect, { passive: true });

  return () => {
    section.removeEventListener('mousemove', handleMouseMove);
    section.removeEventListener('touchmove', handleTouchMove);
    section.removeEventListener('mouseleave', handleLeave);
    section.removeEventListener('touchend', handleLeave);
    window.removeEventListener('resize', invalidateRect);
    window.removeEventListener('scroll', invalidateRect);
  };
}

/** Debounced resize handler. Returns cleanup function. */
export function attachResizeHandler(callback: () => void, delay = 150): () => void {
  let timer: ReturnType<typeof setTimeout>;
  const handler = () => {
    clearTimeout(timer);
    timer = setTimeout(callback, delay);
  };
  window.addEventListener('resize', handler);
  return () => {
    clearTimeout(timer);
    window.removeEventListener('resize', handler);
  };
}

/** Compute two Lissajous attractor positions for a given time. */
export function getAttractorPositions(
  t: number,
  w: number,
  h: number
): { at1X: number; at1Y: number; at2X: number; at2Y: number } {
  return {
    at1X: w * 0.5 + Math.sin(t * 0.25) * w * 0.42,
    at1Y: h * 0.5 + Math.cos(t * 0.18) * h * 0.38,
    at2X: w * 0.5 + Math.cos(t * 0.15) * w * 0.35,
    at2Y: h * 0.5 + Math.sin(t * 0.22) * h * 0.32,
  };
}
