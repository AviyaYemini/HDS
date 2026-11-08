/**
 * A pressable that is also a route link (ADR 014; specs/007-ux-redesign FR-005).
 *
 * `Link asChild` hands its child the link's props through a slot that merges `style` by spreading
 * objects, so a style *function* on the child spreads to nothing and the pressable renders unstyled
 * on the web (its tabs then collapse to their content and fail the 24px target rule). This keeps
 * the interaction state itself and gives the slot one flat style object, so hover, press and focus
 * styling still work and every declared size survives.
 */
import { Link } from "expo-router";
import { useState, type ReactNode } from "react";
import { Pressable, StyleSheet, type PressableProps, type StyleProp, type ViewStyle } from "react-native";

import { mayLeaveFor } from "../unsaved";
import type { PressState } from "./Button";

export interface LinkPressableProps extends Omit<PressableProps, "style" | "children"> {
  href: string;
  style?: StyleProp<ViewStyle> | ((state: PressState) => StyleProp<ViewStyle>);
  children?: ReactNode;
}

export function LinkPressable({ href, style, children, onHoverIn, onHoverOut, onPressIn, onPressOut, onFocus, onBlur, onPress, ...rest }: LinkPressableProps) {
  const [hovered, setHovered] = useState(false);
  const [pressed, setPressed] = useState(false);
  const [focused, setFocused] = useState(false);
  const state: PressState = { hovered, pressed, focused };
  const resolved = StyleSheet.flatten(typeof style === "function" ? style(state) : style) ?? {};
  return (
    <Link href={href as never} asChild>
      <Pressable
        {...rest}
        style={resolved}
        onPress={(event) => {
          onPress?.(event);
          // Unsaved changes on screen: ask first, and stay (FR-045).
          if (!mayLeaveFor(href)) event.preventDefault();
        }}
        onHoverIn={(event) => {
          setHovered(true);
          onHoverIn?.(event);
        }}
        onHoverOut={(event) => {
          setHovered(false);
          onHoverOut?.(event);
        }}
        onPressIn={(event) => {
          setPressed(true);
          onPressIn?.(event);
        }}
        onPressOut={(event) => {
          setPressed(false);
          onPressOut?.(event);
        }}
        onFocus={(event) => {
          setFocused(true);
          onFocus?.(event);
        }}
        onBlur={(event) => {
          setFocused(false);
          onBlur?.(event);
        }}
      >
        {children}
      </Pressable>
    </Link>
  );
}
