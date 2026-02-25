import type { View, ViewContext, AgentState, AgentEvent } from "@agenttrace/viewer/view-api";

// ── CSS injected into the container ──────────────────────────────────────────

const CSS = `
.pm-root {
  background: #0a0a0a;
  color: #c0c0c0;
  font-family: 'Courier New', Courier, monospace;
  font-size: 12px;
  height: 100%;
  overflow: auto;
  padding: 8px;
  box-sizing: border-box;
}
.pm-header {
  background: #111;
  border: 1px solid #2a2a2a;
  padding: 8px 12px;
  margin-bottom: 8px;
  display: flex;
  gap: 24px;
  align-items: center;
  flex-wrap: wrap;
}
.pm-title {
  color: #00ff41;
  font-size: 13px;
  font-weight: bold;
  letter-spacing: 2px;
  text-transform: uppercase;
  white-space: nowrap;
}
.pm-stat { display: flex; flex-direction: column; gap: 1px; }
.pm-stat-label { color: #555; font-size: 9px; text-transform: uppercase; letter-spacing: 1px; }
.pm-stat-value { color: #fff; font-size: 12px; font-weight: bold; }
.pm-stat-value.green { color: #00ff41; }
.pm-stat-value.amber { color: #ffb800; }
.pm-table {
  width: 100%;
  border-collapse: collapse;
  border: 1px solid #1a1a1a;
}
.pm-table thead tr {
  background: #141414;
  border-bottom: 1px solid #2a2a2a;
}
.pm-table th {
  color: #666;
  font-size: 9px;
  text-transform: uppercase;
  letter-spacing: 1px;
  padding: 5px 8px;
  text-align: left;
  white-space: nowrap;
}
.pm-table td {
  padding: 4px 8px;
  border-bottom: 1px solid #111;
  white-space: nowrap;
  vertical-align: middle;
}
.pm-row-active { background: #0a1a0a; }
.pm-row-active:hover { background: #0d230d; cursor: pointer; }
.pm-row-idle { background: #0a0a0a; }
.pm-row-idle:hover { background: #0e0e0e; cursor: pointer; }
.pm-row-idle td { color: #444; }
.pm-row-waiting { background: #1a1500; }
.pm-row-waiting:hover { background: #231e00; cursor: pointer; }
.pm-status-active { color: #00ff41; font-weight: bold; font-size: 10px; }
.pm-status-idle   { color: #333; font-size: 10px; }
.pm-status-waiting { color: #ffb800; font-weight: bold; font-size: 10px; }
.pm-status-done   { color: #2a5a2a; font-size: 10px; }
.pm-tool { color: #7a9fbf; font-size: 11px; max-width: 180px; overflow: hidden; text-overflow: ellipsis; }
.pm-name { color: #c8c8c8; }
.pm-row-idle .pm-name { color: #444; }
.pm-sub { color: #666; font-size: 10px; }
.pm-time { color: #555; font-size: 10px; }
.pm-empty {
  padding: 32px;
  text-align: center;
  color: #2a2a2a;
  border: 1px solid #1a1a1a;
  font-size: 12px;
}
`;

// ── Helpers ───────────────────────────────────────────────────────────────────

function relativeTime(isoTs: string): string {
  const diff = Math.floor((Date.now() - new Date(isoTs).getTime()) / 1000);
  if (diff < 60) return `${diff}s ago`;
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  return `${Math.floor(diff / 3600)}h ago`;
}

function statusClass(status: string): string {
  switch (status) {
    case "active":  return "pm-row-active";
    case "waiting": return "pm-row-waiting";
    default:        return "pm-row-idle";
  }
}

function statusLabel(status: string): string {
  switch (status) {
    case "active":  return '<span class="pm-status-active">ACTIVE</span>';
    case "waiting": return '<span class="pm-status-waiting">WAIT</span>';
    case "done":    return '<span class="pm-status-done">DONE</span>';
    default:        return '<span class="pm-status-idle">IDLE</span>';
  }
}

// ── View state ────────────────────────────────────────────────────────────────

interface RowData {
  agent: AgentState;
  turnCount: number;
  toolCount: number;
}

// ── View implementation ───────────────────────────────────────────────────────

let _ctx: ViewContext | null = null;
let _rootEl: HTMLElement | null = null;
let _styleEl: HTMLStyleElement | null = null;
let _unsubs: Array<() => void> = [];
let _rowData = new Map<string, RowData>();
let _tickTimer: ReturnType<typeof setInterval> | null = null;

function _initRowData(agents: AgentState[]): void {
  _rowData = new Map();
  for (const agent of agents) {
    _rowData.set(agent.id, { agent, turnCount: 0, toolCount: 0 });
  }
}

function _getOrCreate(id: string, agents: AgentState[]): RowData {
  if (!_rowData.has(id)) {
    const agent = agents.find(a => a.id === id);
    if (agent) {
      _rowData.set(id, { agent, turnCount: 0, toolCount: 0 });
    }
  }
  return _rowData.get(id)!;
}

function _renderTable(): void {
  if (!_ctx || !_rootEl) return;

  const rows = Array.from(_rowData.values());
  // Sort: active/waiting first, then by lastActivityAt desc
  rows.sort((a, b) => {
    const aPriority = (a.agent.status === "active" || a.agent.status === "waiting") ? 1 : 0;
    const bPriority = (b.agent.status === "active" || b.agent.status === "waiting") ? 1 : 0;
    if (bPriority !== aPriority) return bPriority - aPriority;
    return new Date(b.agent.lastActivityAt).getTime() - new Date(a.agent.lastActivityAt).getTime();
  });

  const activeCount = rows.filter(r => r.agent.status === "active").length;
  const waitingCount = rows.filter(r => r.agent.status === "waiting").length;
  const totalTools = rows.reduce((sum, r) => sum + r.toolCount, 0);

  const tableBody = _rootEl.querySelector(".pm-tbody") as HTMLElement;
  const statTotal = _rootEl.querySelector(".pm-stat-total") as HTMLElement;
  const statActive = _rootEl.querySelector(".pm-stat-active") as HTMLElement;
  const statWaiting = _rootEl.querySelector(".pm-stat-waiting") as HTMLElement;
  const statTools = _rootEl.querySelector(".pm-stat-tools") as HTMLElement;

  if (statTotal) statTotal.textContent = String(rows.length);
  if (statActive) statActive.textContent = String(activeCount);
  if (statWaiting) statWaiting.textContent = String(waitingCount);
  if (statTools) statTools.textContent = String(totalTools);

  if (!tableBody) return;

  if (rows.length === 0) {
    tableBody.innerHTML = '<tr><td colspan="7" class="pm-empty">No agents connected.</td></tr>';
    return;
  }

  tableBody.innerHTML = rows.map(r => {
    const { agent } = r;
    const rowCls = statusClass(agent.status);
    const subLabel = agent.isSubagent
      ? `<span class="pm-sub">[sub]</span> `
      : "";
    const projectLabel = agent.projectName
      ? `<span style="color:#555;font-size:10px"> ${agent.projectName}</span>`
      : "";
    const toolCell = agent.currentTool
      ? `<span class="pm-tool">${agent.currentTool}</span>`
      : `<span class="pm-time">—</span>`;

    return `<tr class="${rowCls}" data-agent-id="${agent.id}">
      <td class="pm-name">${subLabel}${agent.name}${projectLabel}</td>
      <td>${statusLabel(agent.status)}</td>
      <td>${toolCell}</td>
      <td style="color:#666;font-size:10px">${r.toolCount}</td>
      <td style="color:#666;font-size:10px">${r.turnCount}</td>
      <td class="pm-time">${relativeTime(agent.lastActivityAt)}</td>
      <td class="pm-time" style="font-size:10px;color:#444">${agent.sessionId.slice(0, 12)}</td>
    </tr>`;
  }).join("");

  // Wire click handlers for focus_agent
  tableBody.querySelectorAll("tr[data-agent-id]").forEach(row => {
    (row as HTMLElement).addEventListener("click", () => {
      const agentId = (row as HTMLElement).dataset["agentId"];
      if (agentId && _ctx) {
        _ctx.emit({ type: "focus_agent", agentId });
      }
    });
  });
}

function _buildDOM(container: HTMLElement): void {
  // Inject styles
  _styleEl = document.createElement("style");
  _styleEl.textContent = CSS;
  container.appendChild(_styleEl);

  // Build root
  _rootEl = document.createElement("div");
  _rootEl.className = "pm-root";
  _rootEl.innerHTML = `
    <div class="pm-header">
      <span class="pm-title">Process Monitor</span>
      <div class="pm-stat">
        <span class="pm-stat-label">Agents</span>
        <span class="pm-stat-value pm-stat-total">0</span>
      </div>
      <div class="pm-stat">
        <span class="pm-stat-label">Active</span>
        <span class="pm-stat-value green pm-stat-active">0</span>
      </div>
      <div class="pm-stat">
        <span class="pm-stat-label">Waiting</span>
        <span class="pm-stat-value amber pm-stat-waiting">0</span>
      </div>
      <div class="pm-stat">
        <span class="pm-stat-label">Tools Run</span>
        <span class="pm-stat-value pm-stat-tools">0</span>
      </div>
    </div>
    <table class="pm-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Status</th>
          <th>Current Tool</th>
          <th>Tools</th>
          <th>Turns</th>
          <th>Last Active</th>
          <th>Session</th>
        </tr>
      </thead>
      <tbody class="pm-tbody">
        <tr><td colspan="7" class="pm-empty">No agents connected.</td></tr>
      </tbody>
    </table>
  `;
  container.appendChild(_rootEl);
}

// ── View export ───────────────────────────────────────────────────────────────

const ProcessMonitorView: View = {
  id: "process-monitor",
  name: "Process Monitor",
  description: "htop-style table — best for 10+ concurrent agents",

  mount(ctx: ViewContext): void {
    _ctx = ctx;
    _buildDOM(ctx.container);
    _initRowData(ctx.agents);
    _renderTable();

    // Subscribe to events
    _unsubs.push(ctx.on("agent_created", (e: AgentEvent) => {
      const agent = ctx.agents.find(a => a.id === e.agentId);
      if (agent) {
        _rowData.set(agent.id, { agent, turnCount: 0, toolCount: 0 });
        _renderTable();
      }
    }));

    _unsubs.push(ctx.on("agent_removed", (e: AgentEvent) => {
      _rowData.delete(e.agentId);
      _renderTable();
    }));

    _unsubs.push(ctx.on("subagent_created", (e: AgentEvent) => {
      const agent = ctx.agents.find(a => a.id === e.agentId);
      if (agent) {
        _rowData.set(agent.id, { agent, turnCount: 0, toolCount: 0 });
        _renderTable();
      }
    }));

    _unsubs.push(ctx.on("subagent_removed", (e: AgentEvent) => {
      _rowData.delete(e.agentId);
      _renderTable();
    }));

    _unsubs.push(ctx.on("tool_start", (e: AgentEvent) => {
      const rd = _getOrCreate(e.agentId, ctx.agents);
      if (rd) {
        rd.toolCount += 1;
        // Sync agent reference (status/currentTool may have updated)
        const agent = ctx.agents.find(a => a.id === e.agentId);
        if (agent) rd.agent = agent;
        _renderTable();
      }
    }));

    _unsubs.push(ctx.on("tool_done", (e: AgentEvent) => {
      const rd = _getOrCreate(e.agentId, ctx.agents);
      if (rd) {
        const agent = ctx.agents.find(a => a.id === e.agentId);
        if (agent) rd.agent = agent;
        _renderTable();
      }
    }));

    _unsubs.push(ctx.on("status_changed", (e: AgentEvent) => {
      const rd = _getOrCreate(e.agentId, ctx.agents);
      if (rd) {
        const agent = ctx.agents.find(a => a.id === e.agentId);
        if (agent) rd.agent = agent;
        _renderTable();
      }
    }));

    _unsubs.push(ctx.on("turn_complete", (e: AgentEvent) => {
      const rd = _getOrCreate(e.agentId, ctx.agents);
      if (rd) {
        rd.turnCount += 1;
        const agent = ctx.agents.find(a => a.id === e.agentId);
        if (agent) rd.agent = agent;
        _renderTable();
      }
    }));

    // Tick every 5 seconds to refresh relative timestamps
    _tickTimer = setInterval(() => {
      // Re-sync all agent refs from ctx
      for (const [id, rd] of _rowData) {
        const agent = ctx.agents.find(a => a.id === id);
        if (agent) rd.agent = agent;
      }
      _renderTable();
    }, 5000);
  },

  unmount(): void {
    for (const unsub of _unsubs) {
      unsub();
    }
    _unsubs = [];
    if (_tickTimer !== null) {
      clearInterval(_tickTimer);
      _tickTimer = null;
    }
    if (_rootEl) {
      _rootEl.remove();
      _rootEl = null;
    }
    if (_styleEl) {
      _styleEl.remove();
      _styleEl = null;
    }
    _rowData = new Map();
    _ctx = null;
  },
};

export default ProcessMonitorView;
