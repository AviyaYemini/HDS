/**
 * `LinkPressable` (specs/007-ux-redesign FR-005): a route link that keeps its declared size.
 *
 * expo-router's `Link asChild` passes its props to the child through a slot that merges `style`
 * by spreading objects, which turns a style function into nothing. This test's Link does the same
 * merge, so it fails if the component ever hands the slot a function or an array again.
 */
import { fireEvent, render, screen } from "@testing-library/react-native";
import type { ReactElement } from "react";

import { LinkPressable } from "../../src/design/components/LinkPressable";

jest.mock("expo-router", () => {
  const React = jest.requireActual<typeof import("react")>("react");
  return {
    useRouter: () => ({ replace: jest.fn(), push: jest.fn(), back: jest.fn() }),
    usePathname: () => "/",
    // The slot's merge, as @radix-ui/react-slot does it: objects spread, so a function becomes {}.
    Link: ({ children, href }: { children: ReactElement<{ style?: unknown; href?: string }>; href: string }) => {
      const child = React.Children.only(children);
      return React.cloneElement(child, { href, style: { ...(child.props.style as object) } });
    },
  };
});

describe("LinkPressable", () => {
  it("keeps a function style's values on the rendered pressable", async () => {
    await render(
      <LinkPressable href="/home" testID="tab" style={(state) => [{ minHeight: 60, flex: 1 }, state.pressed ? { opacity: 0.5 } : null]} />,
    );
    const tab = screen.getByTestId("tab");
    expect(tab).toHaveStyle({ minHeight: 60, flex: 1 });
    expect(tab).not.toHaveStyle({ opacity: 0.5 });
    expect(tab.props.href).toBe("/home");
  });

  it("follows press, hover and focus", async () => {
    await render(
      <LinkPressable
        href="/home"
        testID="tab"
        style={(state) => ({ opacity: state.pressed ? 0.5 : 1, borderWidth: state.hovered ? 2 : 0, padding: state.focused ? 4 : 0 })}
      />,
    );
    const tab = screen.getByTestId("tab");
    await fireEvent(tab, "pressIn");
    expect(screen.getByTestId("tab")).toHaveStyle({ opacity: 0.5 });
    await fireEvent(tab, "pressOut");
    expect(screen.getByTestId("tab")).toHaveStyle({ opacity: 1 });
    await fireEvent(tab, "hoverIn");
    expect(screen.getByTestId("tab")).toHaveStyle({ borderWidth: 2 });
    await fireEvent(tab, "focus");
    expect(screen.getByTestId("tab")).toHaveStyle({ padding: 4 });
    await fireEvent(tab, "blur");
    await fireEvent(tab, "hoverOut");
    expect(screen.getByTestId("tab")).toHaveStyle({ borderWidth: 0, padding: 0 });
  });

  it("takes a plain style too", async () => {
    await render(<LinkPressable href="/home" testID="tab" style={{ minHeight: 40 }} />);
    expect(screen.getByTestId("tab")).toHaveStyle({ minHeight: 40 });
  });
});
