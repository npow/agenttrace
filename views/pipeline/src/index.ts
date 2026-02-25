import type { View, ViewContext } from "@agenttrace/viewer/view-api";

const PipelineView: View = {
  id: "pipeline",
  name: "Pipeline",
  description: "DAG view — best for orchestrated workflows with agent dependencies",

  mount(_ctx: ViewContext): void {
    // TODO: DAG nodes + edges, animate data flow between agents
  },

  unmount(): void {
    // TODO: cleanup
  },
};

export default PipelineView;
