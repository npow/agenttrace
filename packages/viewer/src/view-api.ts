/**
 * @agenttrace/viewer — View API
 *
 * This is the stable public interface for view authors.
 * Views implement the View interface and receive a ViewContext on mount.
 * Everything else is an implementation detail of the viewer.
 */

// ── Agent state ──────────────────────────────────────────────────────────────

export type AgentStatus =
  | "idle"       // no active turn
  | "active"     // tool in progress
  | "waiting"    // awaiting permission
  | "done";      // turn complete, fading

export interface AgentState {
  id: string;
  name: string;
  status: AgentStatus;
  currentTool?: string;
  sessionId: string;
  projectName?: string;
  startedAt: string;        // ISO timestamp
  lastActivityAt: string;   // ISO timestamp
  parentId?: string;        // set if this is a sub-agent
  isSubagent: boolean;
}

// ── Events ───────────────────────────────────────────────────────────────────

export type AgentEventType =
  | "agent_created"
  | "agent_removed"
  | "tool_start"
  | "tool_done"
  | "status_changed"
  | "turn_complete"
  | "subagent_created"
  | "subagent_removed";

export interface AgentEvent {
  type: AgentEventType;
  agentId: string;
  timestamp: string;        // ISO timestamp
  payload?: Record<string, unknown>;
}

// ── Replay ───────────────────────────────────────────────────────────────────

export interface ReplayControls {
  play(): void;
  pause(): void;
  seek(timestamp: string): void;
  setSpeed(multiplier: number): void;
  readonly isPlaying: boolean;
  readonly currentTime: string;
  readonly startTime: string;
  readonly endTime: string;
}

// ── View context ──────────────────────────────────────────────────────────────

export interface ViewContext {
  /** Current snapshot of all agent states */
  agents: AgentState[];

  /** DOM container — view owns this element entirely */
  container: HTMLElement;

  /**
   * Subscribe to agent events.
   * Returns an unsubscribe function.
   */
  on(event: AgentEventType, cb: (event: AgentEvent) => void): () => void;

  /**
   * Subscribe to all events regardless of type.
   * Returns an unsubscribe function.
   */
  onAny(cb: (event: AgentEvent) => void): () => void;

  /**
   * Emit a UI action back to the host (e.g. user clicked an agent).
   * The host decides what to do with it (focus terminal, show details, etc.)
   */
  emit(action: ViewAction): void;

  /** Present only in replay mode */
  replay?: ReplayControls;
}

// ── View actions (view → host) ────────────────────────────────────────────────

export type ViewAction =
  | { type: "focus_agent"; agentId: string }
  | { type: "dismiss_agent"; agentId: string }
  | { type: "custom"; name: string; payload?: unknown };

// ── View interface ────────────────────────────────────────────────────────────

export interface View {
  /** Unique identifier — used for persistence, URL routing, etc. */
  id: string;

  /** Human-readable name shown in the view switcher */
  name: string;

  description?: string;

  /**
   * Called once when the view becomes active.
   * The view should render into ctx.container and set up event listeners.
   */
  mount(ctx: ViewContext): void | Promise<void>;

  /**
   * Called when the view is deactivated or the viewer is unmounted.
   * Clean up timers, event listeners, canvas contexts, etc.
   */
  unmount(): void | Promise<void>;
}

// ── View manifest (for dynamic loading) ──────────────────────────────────────

export interface ViewManifest {
  id: string;
  name: string;
  description?: string;
  version: string;
  /** Resolved path or URL to the ES module exporting a default View */
  entry: string;
}
