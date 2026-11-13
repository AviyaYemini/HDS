/** The one part of React DOM the web build uses (Portal.web.tsx); the package ships without types. */
declare module "react-dom" {
  import type { ReactNode, ReactPortal } from "react";

  export function createPortal(children: ReactNode, container: Element | DocumentFragment): ReactPortal;
}
