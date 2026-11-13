/**
 * Keep focused content clear of a bar stuck to the bottom of a phone's screen (WCAG 2.4.11, Focus
 * Not Obscured): on the web, the scroll area that holds the bar gets a bottom scroll padding the
 * bar's height, so the browser scrolls a focused element above the bar rather than under it. It
 * is put back when the bar goes. Native screens scroll a focused element into view on their own.
 */
import { useEffect, type ComponentRef, type RefObject } from "react";
import { Platform, type View } from "react-native";

/** The gap kept between a focused element and the bar, in px. */
const GAP = 8;

export function useStickyClearance(bar: RefObject<ComponentRef<typeof View> | null>, on: boolean): void {
  useEffect(() => {
    if (Platform.OS !== "web" || !on) return undefined;
    // On the web a View's ref is its element.
    const node = bar.current as unknown as HTMLElement | null;
    if (!node) return undefined;
    let area = node.parentElement;
    while (area && !/(auto|scroll)/.test(getComputedStyle(area).overflowY)) area = area.parentElement;
    const target = area ?? document.documentElement;
    const before = target.style.getPropertyValue("scroll-padding-bottom");
    const apply = () => target.style.setProperty("scroll-padding-bottom", `${Math.ceil(node.getBoundingClientRect().height) + GAP}px`);
    apply();
    // The bar grows when its labels wrap (a narrower screen, another language): follow its height.
    const observer = typeof ResizeObserver === "undefined" ? null : new ResizeObserver(apply);
    observer?.observe(node);
    return () => {
      observer?.disconnect();
      target.style.setProperty("scroll-padding-bottom", before);
    };
  }, [bar, on]);
}
