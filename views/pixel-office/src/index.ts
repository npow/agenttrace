import type { View, ViewContext } from "@agenttrace/viewer/view-api";

const PixelOfficeView: View = {
  id: "pixel-office",
  name: "Pixel Office",
  description: "Animated pixel art office — agents as characters at desks",

  mount(_ctx: ViewContext): void {
    // TODO: canvas renderer + character FSM
  },

  unmount(): void {
    // TODO: cleanup canvas, cancel rAF loop
  },
};

export default PixelOfficeView;
