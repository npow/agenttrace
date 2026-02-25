import type { View, ViewContext } from "@agenttrace/viewer/view-api";

const MissionControlView: View = {
  id: "mission-control",
  name: "Mission Control",
  description: "Status cards grid — best for 5–12 agents, permission approval inline",

  mount(_ctx: ViewContext): void {
    // TODO: card grid, highlight waiting agents, inline approve/deny
  },

  unmount(): void {
    // TODO: cleanup
  },
};

export default MissionControlView;
