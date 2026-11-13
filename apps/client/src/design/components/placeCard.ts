/**
 * Where a floating card goes beside a point (the owner, 2026-11-13): after the point and below it,
 * flipped to the other side when it would leave the window, and shifted to stay inside it. In
 * window pixels, so the same in a right-to-left and a left-to-right layout.
 */
export function placeCard(
  point: { x: number; y: number },
  card: { width: number; height: number },
  window: { width: number; height: number },
  gap = 12,
  margin = 8,
): { left: number; top: number } {
  let left = point.x + gap;
  if (left + card.width > window.width - margin) left = point.x - gap - card.width;
  let top = point.y + gap;
  if (top + card.height > window.height - margin) top = point.y - gap - card.height;
  const clamp = (v: number, size: number, room: number) => Math.max(margin, Math.min(v, room - size - margin));
  return { left: clamp(left, card.width, window.width), top: clamp(top, card.height, window.height) };
}
