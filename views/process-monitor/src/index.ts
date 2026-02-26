import type { View, ViewContext, AgentState, AgentEvent } from "@agenttrace/viewer/view-api";

// ── constants ─────────────────────────────────────────────────────────────────

const STUCK_MS      = 3 * 60 * 1000;
const LONG_MS       = 60 * 1000;
const TICK_MS       = 1_000;
const TL_WINDOW     = 8 * 60 * 60 * 1000;   // 8h timeline window
const TL_REFRESH_MS = 10_000;                // refresh tool data in tl/tree every 10s

// ── CSS ───────────────────────────────────────────────────────────────────────

const CSS = `
* { box-sizing: border-box; margin: 0; padding: 0; }
.pm { height: 100%; overflow-y: auto; background: #0d1117; color: #c9d1d9;
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
      font-size: 14px; line-height: 1.5; }

/* header */
.pm-hdr { position: sticky; top: 0; z-index: 10; background: #0d1117;
          border-bottom: 1px solid #21262d; padding: 7px 14px;
          display: flex; align-items: center; gap: 16px; }
.pm-hdr-title { font-size: 11px; font-weight: 700; letter-spacing: .08em;
                text-transform: uppercase; color: #6e7681; }
.pm-stat { display: flex; align-items: baseline; gap: 4px; }
.pm-stat-n { font-size: 14px; font-weight: 700; color: #e6edf3; }
.pm-stat-n.c-active { color: #3fb950; }
.pm-stat-n.c-stuck  { color: #f85149; }
.pm-stat-l { font-size: 12px; color: #6e7681; }

/* tab buttons */
.pm-tabs { margin-left: auto; display: flex; gap: 2px; }
.pm-tab { font-size: 11px; padding: 3px 10px; border-radius: 4px; cursor: pointer;
          border: 1px solid #30363d; background: transparent; color: #8b949e; }
.pm-tab:hover { background: #161b22; color: #c9d1d9; }
.pm-tab.active { background: #1c2128; border-color: #388bfd; color: #58a6ff; }

/* ── LIST MODE ── */
.pm-list { padding: 4px 0 20px; }
.pm-row { display: flex; align-items: center; gap: 8px; padding: 4px 14px;
          cursor: pointer; min-height: 26px; user-select: none; }
.pm-row:hover { background: #161b22; }
.pm-row.r-stuck { background: #140c0c; }
.pm-row.r-stuck:hover { background: #1a1010; }
.pm-pfx { color: #30363d; white-space: pre; flex-shrink: 0; letter-spacing: 0;
          font-family: 'Menlo', 'Monaco', 'SF Mono', monospace; font-size: 12px; }
.pm-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
.pm-dot.s-active { background: #3fb950; box-shadow: 0 0 5px #3fb95055; }
.pm-dot.s-stuck  { background: #f85149; box-shadow: 0 0 5px #f8514955;
                   animation: pm-blink .8s ease infinite; }
.pm-dot.s-done   { background: #1a3a27; }
.pm-dot.s-idle   { background: #21262d; }
@keyframes pm-blink { 0%,100%{opacity:1} 50%{opacity:.3} }
.pm-name { font-size: 13px; white-space: nowrap; overflow: hidden;
           text-overflow: ellipsis; color: #e6edf3; flex: 1; min-width: 0; max-width: 300px; }
.pm-name.dim { color: #6e7681; }
.pm-right { flex: 1; display: flex; align-items: center; gap: 8px;
            min-width: 0; overflow: hidden; }
.pm-tool-badge { font-size: 10px; padding: 1px 5px; border-radius: 3px;
                 background: #0d1f3c; border: 1px solid #1c3a6b; color: #58a6ff;
                 white-space: nowrap; flex-shrink: 0;
                 font-family: 'Menlo','Monaco','SF Mono',monospace; }
.pm-ctx { font-size: 12px; color: #8b949e; white-space: nowrap; overflow: hidden;
          text-overflow: ellipsis; flex: 1; min-width: 0; }
.pm-ctx.ctx-active { color: #c9d1d9; }
.pm-bar { flex: 1; max-width: 64px; height: 2px; background: #21262d;
          border-radius: 1px; overflow: hidden; flex-shrink: 0; }
.pm-bar-fill { height: 100%; border-radius: 1px; }
.pm-bar-fill.c-ok    { background: #3fb950; }
.pm-bar-fill.c-long  { background: #d29922; }
.pm-bar-fill.c-stuck { background: #f85149; }
.pm-dur { font-size: 12px; white-space: nowrap; flex-shrink: 0; min-width: 50px;
          text-align: right; font-family: 'Menlo','Monaco','SF Mono',monospace; }
.pm-dur.c-ok    { color: #6e7681; }
.pm-dur.c-long  { color: #d29922; }
.pm-dur.c-stuck { color: #f85149; font-weight: 700; }
.pm-ago { font-size: 12px; color: #6e7681; white-space: nowrap;
          flex-shrink: 0; margin-left: auto; }
.pm-chevron { font-size: 10px; color: #6e7681; flex-shrink: 0; width: 14px;
              text-align: center; transition: transform .15s; }
.pm-chevron.open { transform: rotate(90deg); color: #8b949e; }
.pm-row.r-focused { background: #1c2128; outline: 1px solid #388bfd44; }
.pm-row.r-stuck.r-focused { background: #1f1010; }
/* fixed-width meta area so pills align consistently across all rows */
.pm-meta { display: flex; align-items: center; gap: 6px; flex-shrink: 0;
           margin-left: 8px; min-width: 160px; justify-content: flex-end; }
.pm-chip { font-size: 11px; padding: 1px 6px; border-radius: 10px;
           background: #161b22; border: 1px solid #30363d; color: #8b949e; white-space: nowrap; }
.pm-chip.ch-cost   { color: #3fb950; border-color: #1a3a27; background: #0d1f14; }
.pm-chip.ch-err    { color: #f85149; border-color: #3c1212; background: #1c0d0d; }
.pm-chip.ch-streak { color: #f85149; border-color: #3c1212; background: #200808; font-weight:700; }
.pm-chip.ch-ctx    { color: #d29922; border-color: #3d2e00; background: #130f00; }
.pm-chip.ch-turns  { color: #8b949e; }

/* subagent panel */
.pm-subagents { background: #010409; border-top: 1px solid #161b22;
                border-bottom: 1px solid #161b22; padding: 4px 0; }
.pm-sa-row { display: flex; flex-direction: column;
             padding: 3px 14px 3px 36px; min-height: 22px; }
.pm-sa-row:hover { background: #0d1117; }
.pm-sa-main { display: flex; align-items: center; gap: 8px; }
.pm-sa-dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
.pm-sa-dot.sa-done { background: #238636; }
.pm-sa-dot.sa-err  { background: #f85149; }
.pm-sa-dot.sa-pend { background: #484f58; }
.pm-sa-badge { font-size: 10px; padding: 1px 5px; border-radius: 3px;
               background: #0d1f3c; border: 1px solid #1c3a6b; color: #58a6ff;
               white-space: nowrap; flex-shrink: 0;
               font-family: 'Menlo','Monaco','SF Mono',monospace; }
.pm-sa-badge.tool-bash { background: #1c1100; border-color: #3d2600; color: #e3b341; }
.pm-sa-badge.tool-web  { background: #0d1c2c; border-color: #1c3a52; color: #79c0ff; }
.pm-sa-badge.tool-file { background: #0d1c14; border-color: #1c3a28; color: #56d364; }
.pm-sa-prompt { font-size: 12px; color: #8b949e; white-space: nowrap; overflow: hidden;
                text-overflow: ellipsis; flex: 1; min-width: 0; }
.pm-sa-dur { font-size: 11px; color: #6e7681; white-space: nowrap; flex-shrink: 0;
             font-family: 'Menlo','Monaco','SF Mono',monospace; }
.pm-sa-out { font-size: 11px; color: #484f58; padding-left: 22px; margin-top: 1px;
             font-family: 'Menlo','Monaco','SF Mono',monospace;
             white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
.pm-sa-loading { padding: 6px 36px; font-size: 12px; color: #6e7681; font-style: italic; }
.pm-sa-empty   { padding: 6px 36px; font-size: 12px; color: #484f58; }

.pm-empty { padding: 40px 14px; text-align: center; color: #484f58; font-size: 13px; }

/* ── TIMELINE MODE ── */
.pm-tl { padding: 12px 14px 40px; }
.pm-tl-axis { position: relative; height: 20px; margin-left: 180px;
              border-bottom: 1px solid #21262d; margin-bottom: 8px; }
.pm-tl-tick { position: absolute; bottom: 0; font-size: 10px; color: #6e7681;
              transform: translateX(-50%); white-space: nowrap; }
.pm-tl-tick::before { content: ''; position: absolute; bottom: 100%;
                      left: 50%; width: 1px; height: 4px; background: #21262d; }
.pm-tl-lane { display: flex; align-items: center; margin: 6px 0; min-height: 36px; }
.pm-tl-label { width: 176px; padding-right: 8px; flex-shrink: 0;
               font-size: 12px; text-align: right; overflow: hidden;
               text-overflow: ellipsis; white-space: nowrap; }
.pm-tl-label .tl-proj { color: #6e7681; font-size: 10px; display: block; }
.pm-tl-label .tl-name { color: #9ea7b3; }
.pm-tl-label.tl-active .tl-name { color: #e6edf3; }
.pm-tl-track { flex: 1; position: relative; height: 36px; overflow: visible; }
/* now indicator */
.pm-tl-now { position: absolute; top: 0; bottom: 0; width: 1px;
             background: #f8514966; z-index: 5; pointer-events: none; }
.pm-tl-now::after { content: 'now'; position: absolute; top: -14px; right: 3px; left: auto;
                    font-size: 9px; color: #f85149; white-space: nowrap; }
/* session base bar */
.pm-tl-sbar { position: absolute; top: 14px; height: 8px; border-radius: 3px;
              background: #21262d; border: 1px solid #444d56; }
.pm-tl-sbar.tl-active { background: #0d2a0d; border-color: #238636; }
/* task blocks — tall, above session bar */
.pm-tl-task { position: absolute; top: 4px; height: 28px; border-radius: 2px;
              min-width: 3px; cursor: pointer; border: 1px solid transparent;
              transition: opacity .1s; }
.pm-tl-task:hover { opacity: .85; z-index: 10; }
/* track drag-to-zoom */
.pm-tl-track { cursor: crosshair; }
.pm-tl-sel { position: fixed; pointer-events: none; z-index: 99;
             background: rgba(56,139,253,0.10); border: 1px solid rgba(56,139,253,0.55);
             border-radius: 1px; }
/* tool type colors */
.tl-t-task    { background: #1f4080; border-color: #3878d4; }
.tl-t-bash    { background: #4d3000; border-color: #996000; }
.tl-t-web     { background: #0e3549; border-color: #1c6691; }
.tl-t-file    { background: #1c3c1c; border-color: #2d6b2d; }
.tl-t-other   { background: #21262d; border-color: #444d56; }
.tl-t-err     { background: #4d1515; border-color: #993333; }
/* parallel swim lanes within a session */
.pm-tl-task.lane-1 { top: 4px; }
.pm-tl-task.lane-2 { top: 18px; height: 14px; }
/* tooltip */
.pm-tl-tip { position: fixed; background: #161b22; border: 1px solid #30363d;
             border-radius: 6px; padding: 8px 12px; font-size: 12px; color: #c9d1d9;
             pointer-events: none; z-index: 100; max-width: 320px; display: none;
             white-space: pre-wrap; line-height: 1.4; }
/* zoom reset bar */
.pm-tl-zoom-bar { display: flex; align-items: center; gap: 8px;
                  padding: 4px 14px; border-bottom: 1px solid #21262d;
                  font-size: 11px; color: #6e7681; }
.pm-tl-zoom-btn { font-size: 11px; padding: 1px 8px; border-radius: 4px; cursor: pointer;
                  border: 1px solid #30363d; background: #161b22; color: #8b949e; }
.pm-tl-zoom-btn:hover { background: #1c2128; color: #c9d1d9; }
/* lane hover highlight */
.pm-tl-label { cursor: pointer; }
.pm-tl-lane:hover .pm-tl-label .tl-name { color: #e6edf3; }

/* ── STATS MODE ── */
.pm-dag { display: flex; height: 100%; overflow: hidden; }
/* shared sidebar */
.pm-dag-sidebar { width: 200px; flex-shrink: 0; overflow-y: auto;
                  border-right: 1px solid #21262d; padding: 4px 0; }
.pm-dag-sess { display: flex; align-items: center; gap: 6px;
               padding: 5px 10px; cursor: pointer; user-select: none; }
.pm-dag-sess:hover { background: #161b22; }
.pm-dag-sess.selected { background: #1c2128; border-left: 2px solid #388bfd; }
.pm-dag-sess-name { font-size: 12px; color: #8b949e; white-space: nowrap;
                    overflow: hidden; text-overflow: ellipsis; flex: 1; min-width: 0; }
.pm-dag-sess-name.active { color: #e6edf3; }
/* main stats panel */
.pm-dag-main { flex: 1; overflow-y: auto; padding: 16px 20px 40px; }
.pm-dag-empty { color: #484f58; font-size: 13px; padding: 40px; text-align: center; }
/* summary kv row */
.pm-st-kvrow { display: flex; gap: 0; margin-bottom: 20px; }
.pm-st-kv { flex: 1; text-align: center; padding: 8px 0;
            border-right: 1px solid #161b22; }
.pm-st-kv:last-child { border-right: none; }
.pm-st-n { font-size: 22px; font-weight: 700; color: #e6edf3; line-height: 1.1; }
.pm-st-n.c-active { color: #3fb950; }
.pm-st-n.c-stuck  { color: #f85149; }
.pm-st-l { font-size: 11px; color: #6e7681; margin-top: 2px; }
/* section header */
.pm-st-hdr { font-size: 11px; font-weight: 600; color: #6e7681;
             text-transform: uppercase; letter-spacing: .06em;
             margin: 0 0 8px; padding-bottom: 4px; border-bottom: 1px solid #21262d; }
/* tool table */
.pm-st-tbl { width: 100%; border-collapse: collapse; font-size: 12px; margin-bottom: 20px; }
.pm-st-tbl th { text-align: left; color: #6e7681; padding: 0 10px 5px 0;
                font-size: 11px; font-weight: 600; }
.pm-st-tbl td { padding: 4px 10px 4px 0; color: #c9d1d9;
                border-top: 1px solid #161b22; vertical-align: middle; }
.pm-st-tbl tr:hover td { background: #0d1117; }
.pm-st-bar-bg { background: #21262d; border-radius: 2px; height: 4px; width: 60px; }
.pm-st-bar-fg { height: 4px; border-radius: 2px; background: #388bfd; }
/* error list */
.pm-st-err { margin-bottom: 20px; }
.pm-st-err-row { padding: 5px 0; border-bottom: 1px solid #161b22; font-size: 12px; }
.pm-st-err-tool { color: #f85149; font-family: 'Menlo','Monaco','SF Mono',monospace;
                  font-size: 11px; }
.pm-st-err-prompt { color: #8b949e; margin-left: 6px; }
.pm-st-err-out { color: #6e7681; font-size: 11px; margin-top: 2px;
                 font-family: 'Menlo','Monaco','SF Mono',monospace; }
/* retro analysis panel */
.pm-retro { background: #0d1a12; border: 1px solid #1e3a28; border-radius: 6px;
            padding: 10px 14px; margin-bottom: 20px; }
.pm-retro-hdr { font-size: 10px; font-weight: 600; color: #6e7681;
                text-transform: uppercase; letter-spacing: .06em; margin-bottom: 8px; }
.pm-retro-badge { display: inline-flex; align-items: center; gap: 5px;
                  font-size: 11px; font-weight: 600; padding: 2px 8px;
                  border-radius: 10px; margin-bottom: 8px; }
.pm-retro-badge.o-success { background: #0d2a0d; color: #3fb950; border: 1px solid #238636; }
.pm-retro-badge.o-failure { background: #2d0d0d; color: #f85149; border: 1px solid #6e3030; }
.pm-retro-badge.o-partial { background: #2a1d00; color: #d29922; border: 1px solid #6a4800; }
.pm-retro-badge.o-unknown { background: #1c2128; color: #8b949e; border: 1px solid #30363d; }
.pm-retro-narrative { font-size: 12px; color: #9198a1; line-height: 1.5; margin-bottom: 10px; }
.pm-retro-scores { display: flex; gap: 12px; margin-bottom: 8px; }
.pm-retro-score { flex: 1; }
.pm-retro-score-l { font-size: 10px; color: #6e7681; margin-bottom: 3px; }
.pm-retro-score-t { height: 4px; background: #21262d; border-radius: 2px; overflow: hidden; }
.pm-retro-score-f { height: 4px; border-radius: 2px; }
.pm-retro-loading { font-size: 11px; color: #484f58; font-style: italic; }
`;

// ── types ─────────────────────────────────────────────────────────────────────

interface AgentRow {
  agent: AgentState;
  toolStartedAt: number | null;
  errorCount: number;
  turnCount: number;
  costUsd: number;
  errorStreak: number;
  ctxPct: number;
  context: string;
}

interface SubAgent {
  id: string;
  tool: string;
  prompt: string;
  output: string;
  completed: boolean;
  error: boolean;
  startedAt: string;
  finishedAt: string | null;
  durationMs: number | null;
}

interface FlatNode { row: AgentRow; prefix: string; }

// ── module state ──────────────────────────────────────────────────────────────

let _ctx:      ViewContext | null      = null;
let _root:     HTMLElement | null      = null;
let _style:    HTMLStyleElement | null = null;
let _unsubs:   Array<() => void>       = [];
let _tick:     ReturnType<typeof setInterval> | null = null;
let _tlTick:   ReturnType<typeof setInterval> | null = null;
let _rows      = new Map<string, AgentRow>();
let _focusIdx  = -1;
let _expanded  = new Map<string, SubAgent[] | null>();
let _mode: 'list' | 'timeline' | 'dag' = 'list';
let _tlData    = new Map<string, SubAgent[]>();   // timeline cache
let _tlLoading = false;
let _tip:      HTMLElement | null = null;
let _dagSel:      string | null = null;           // selected session in stats view
let _statsLoading = new Set<string>();            // sessions with in-flight data fetches
let _retroData    = new Map<string, any | null>(); // retro session analysis cache (null = unavailable)
let _retroLoading = new Set<string>();             // in-flight retro fetches
let _tlZoom:   { start: number; end: number } | null = null;  // null = full 8h window
let _tlSelDiv: HTMLElement | null = null;                     // drag-to-zoom overlay rect

// ── helpers ───────────────────────────────────────────────────────────────────

function esc(s: string): string {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

function shortProject(raw: string): string {
  return raw.replace(/^-[Uu]sers-[^-]+-(?:code|work|dev|src|projects?)-/, "").replace(/^-/, "");
}

function relTime(iso: string): string {
  const s = Math.floor((Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60)   return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  return `${Math.floor(s / 3600)}h ago`;
}

function durFmt(ms: number): string {
  const s = Math.floor(ms / 1000);
  if (s < 60) return `${s}s`;
  return `${Math.floor(s / 60)}m ${String(s % 60).padStart(2, "0")}s`;
}

function fmtTickLabel(ms: number, intervalMs: number): string {
  const d = new Date(ms);
  const hh = String(d.getHours()).padStart(2,"0");
  const mm = String(d.getMinutes()).padStart(2,"0");
  const ss = String(d.getSeconds()).padStart(2,"0");
  return intervalMs < 60_000 ? `${hh}:${mm}:${ss}` : `${hh}:${mm}`;
}

// Returns evenly-spaced ticks aligned to natural intervals
function tlTicks(start: number, end: number): Array<{ pct: number; label: string }> {
  const dur = end - start;
  const INTERVALS = [
    1_000, 5_000, 10_000, 30_000,
    60_000, 5*60_000, 10*60_000, 15*60_000, 30*60_000,
    3600_000, 2*3600_000, 4*3600_000,
  ];
  const target = dur / 8;
  const interval = INTERVALS.find(i => i >= target) ?? INTERVALS[INTERVALS.length - 1];
  const first = Math.ceil(start / interval) * interval;
  const ticks: Array<{ pct: number; label: string }> = [];
  for (let t = first; t <= end; t += interval) {
    ticks.push({ pct: (t - start) / dur * 100, label: fmtTickLabel(t, interval) });
  }
  return ticks;
}

function getTimeWindow(): { start: number; end: number } {
  if (_tlZoom) return _tlZoom;
  return { start: Date.now() - TL_WINDOW, end: Date.now() };
}

function durClass(ms: number): "c-ok" | "c-long" | "c-stuck" {
  return ms >= STUCK_MS ? "c-stuck" : ms >= LONG_MS ? "c-long" : "c-ok";
}

function barPct(ms: number): number { return Math.min(100, (ms / STUCK_MS) * 100); }

async function fetchSubagents(sessionId: string): Promise<SubAgent[]> {
  const res = await fetch(`/api/session/${sessionId}/subagents`);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return ((await res.json()).agents) as SubAgent[];
}

// Fetch retro analysis for a completed session (may 404 if session not yet analysed)
async function fetchRetroSession(sessionId: string): Promise<any | null> {
  try {
    const res = await fetch(`/api/sessions/${sessionId}`);
    if (!res.ok) return null;
    return await res.json();
  } catch { return null; }
}

function toggleExpand(sessionId: string): void {
  if (_expanded.has(sessionId)) { _expanded.delete(sessionId); render(); return; }
  _expanded.set(sessionId, null);
  render();
  fetchSubagents(sessionId).then(a => { _expanded.set(sessionId, a); render(); }).catch(() => {});
}

function toolBadgeClass(tool: string): string {
  const t = tool.toLowerCase();
  if (t === "bash")                              return "tool-bash";
  if (t === "websearch" || t === "webfetch")     return "tool-web";
  if (["read","edit","write","glob","grep","notebook","notepadread","notepadwrite"].includes(t))
                                                 return "tool-file";
  return "";
}


// ── row sync ──────────────────────────────────────────────────────────────────

function syncRow(id: string): void {
  const row = _rows.get(id);
  if (!row || !_ctx) return;
  const a = _ctx.agents.find(a => a.id === id);
  if (!a) return;
  row.agent = a;
  const x = a as AgentState & Record<string, unknown>;
  const apiTs = x["toolStartedAt"] as string | null | undefined;
  row.toolStartedAt = apiTs ? new Date(apiTs).getTime() : null;
  row.turnCount   = (x["turnCount"]   as number) ?? row.turnCount;
  row.costUsd     = (x["costUsd"]     as number) ?? row.costUsd;
  row.errorCount  = (x["errorCount"]  as number) ?? row.errorCount;
  row.errorStreak = (x["errorStreak"] as number) ?? row.errorStreak;
  row.ctxPct      = (x["ctxPct"]      as number) ?? row.ctxPct;
  row.context     = (x["context"]     as string) ?? row.context;
}

function rowFromAgent(a: AgentState): AgentRow {
  const x = a as AgentState & Record<string, unknown>;
  const apiTs = x["toolStartedAt"] as string | null | undefined;
  return {
    agent: a, toolStartedAt: apiTs ? new Date(apiTs).getTime() : null,
    errorCount: (x["errorCount"] as number) ?? 0,
    turnCount:  (x["turnCount"]  as number) ?? 0,
    costUsd:    (x["costUsd"]    as number) ?? 0,
    errorStreak:(x["errorStreak"]as number) ?? 0,
    ctxPct:     (x["ctxPct"]     as number) ?? 0,
    context:    (x["context"]    as string) ?? "",
  };
}

function getOrCreate(id: string): AgentRow | null {
  if (!_ctx) return null;
  if (!_rows.has(id)) {
    const a = _ctx.agents.find(a => a.id === id);
    if (!a) return null;
    _rows.set(id, rowFromAgent(a));
  }
  return _rows.get(id)!;
}

// ── tree builder ──────────────────────────────────────────────────────────────

function buildFlat(): FlatNode[] {
  if (!_ctx) return [];
  const agents = _ctx.agents;
  const ids    = new Set(agents.map(a => a.id));
  const roots  = agents.filter(a => !a.parentId || !ids.has(a.parentId));
  const result: FlatNode[] = [];
  function visit(agent: AgentState, inherited: string, isLast: boolean, depth: number): void {
    const connector = depth === 0 ? "" : (isLast ? "└─ " : "├─ ");
    result.push({ row: _rows.get(agent.id) ?? rowFromAgent(agent), prefix: inherited + connector });
    const children = agents.filter(a => a.parentId === agent.id);
    const next = depth === 0 ? "" : inherited + (isLast ? "   " : "│  ");
    children.forEach((c, i) => visit(c, next, i === children.length - 1, depth + 1));
  }
  roots.forEach((r, i) => visit(r, "", i === roots.length - 1, 0));
  return result;
}

// ── subagent panel (list mode) ────────────────────────────────────────────────

function renderSubagentPanel(sessionId: string): string {
  const data = _expanded.get(sessionId);
  if (data == null)
    return `<div class="pm-subagents"><div class="pm-sa-loading">Loading…</div></div>`;
  if (data.length === 0)
    return `<div class="pm-subagents"><div class="pm-sa-empty">No tool calls recorded</div></div>`;

  const rows = data.slice(-40).map(sa => {
    const dotCls  = sa.error ? "sa-err" : sa.completed ? "sa-done" : "sa-pend";
    const badgeCls = toolBadgeClass(sa.tool);
    const dur = sa.durationMs != null ? durFmt(sa.durationMs) : sa.completed ? "?" : "…";
    const outText = sa.output ? sa.output.replace(/\n/g, " ").slice(0, 140) : "";
    const outHtml = outText
      ? `<div class="pm-sa-out">→ ${esc(outText)}</div>`
      : "";
    return `<div class="pm-sa-row">
      <div class="pm-sa-main">
        <span class="pm-sa-dot ${dotCls}"></span>
        <span class="pm-sa-badge ${badgeCls}">${esc(sa.tool)}</span>
        <span class="pm-sa-prompt">${esc(sa.prompt || (sa.completed ? "(no preview)" : "running…"))}</span>
        <span class="pm-sa-dur">${esc(dur)}</span>
      </div>
      ${outHtml}
    </div>`;
  }).join("");
  return `<div class="pm-subagents">${rows}</div>`;
}

// ── list row renderer ─────────────────────────────────────────────────────────

function renderRow({ row, prefix }: FlatNode, focused: boolean): string {
  const { agent, toolStartedAt, errorCount, turnCount, costUsd } = row;
  const elapsedMs = toolStartedAt ? Date.now() - toolStartedAt : 0;
  const isStuck   = toolStartedAt !== null && elapsedMs >= STUCK_MS;
  const dc        = toolStartedAt ? durClass(elapsedMs) : "c-ok";
  const isActive  = agent.status === "active" || isStuck;
  const isExpanded = isActive || _expanded.has(agent.id);

  const dotClass = isStuck ? "s-stuck"
    : agent.status === "active" ? "s-active"
    : agent.status === "done"   ? "s-done" : "s-idle";

  const ctx = row.context || "";
  let rightHtml: string;
  if (agent.currentTool && toolStartedAt) {
    rightHtml = `<span class="pm-tool-badge">${esc(agent.currentTool)}</span>
      ${ctx ? `<span class="pm-ctx ctx-active">${esc(ctx)}</span>` : ""}
      <div class="pm-bar"><div class="pm-bar-fill ${dc}" style="width:${barPct(elapsedMs).toFixed(0)}%"></div></div>
      <span class="pm-dur ${dc}">${durFmt(elapsedMs)}</span>`;
  } else if (agent.currentTool) {
    rightHtml = `<span class="pm-tool-badge">${esc(agent.currentTool)}</span>
      ${ctx ? `<span class="pm-ctx ctx-active">${esc(ctx)}</span>` : ""}`;
  } else if (ctx) {
    rightHtml = `<span class="pm-ctx">${esc(ctx)}</span>
      <span class="pm-ago">${relTime(agent.lastActivityAt)}</span>`;
  } else {
    rightHtml = `<span class="pm-ago">${relTime(agent.lastActivityAt)}</span>`;
  }

  const proj = agent.projectName ? shortProject(agent.projectName) : "";
  const costStr = costUsd >= 0.01 ? `$${costUsd.toFixed(2)}` : costUsd > 0 ? `$${(costUsd * 100).toFixed(1)}¢` : "";
  const streak = row.errorStreak, ctxPct = row.ctxPct;
  const meta = `<div class="pm-meta">
    ${turnCount > 0 ? `<span class="pm-chip ch-turns">${turnCount}t</span>` : ""}
    ${costStr       ? `<span class="pm-chip ch-cost">${esc(costStr)}</span>` : ""}
    ${streak >= 3   ? `<span class="pm-chip ch-streak">${streak}✕ err</span>` : ""}
    ${ctxPct >= 50  ? `<span class="pm-chip ch-ctx">${ctxPct}% ctx</span>` : ""}
    ${errorCount > 0 && streak < 3 ? `<span class="pm-chip ch-err">${errorCount} err</span>` : ""}
  </div>`;

  const chevOpen = isExpanded && !isActive;
  const chevron = `<span class="pm-chevron${chevOpen ? " open" : ""}">▶</span>`;

  return `
    <div class="pm-row${isStuck?" r-stuck":""}${focused?" r-focused":""}" data-id="${esc(agent.id)}" data-action="expand">
      ${chevron}
      <span class="pm-pfx">${esc(prefix)}</span>
      <span class="pm-dot ${dotClass}"></span>
      <span class="pm-name${agent.status==="idle"||agent.status==="done"?" dim":""}" title="${esc(agent.name)}">${esc(proj || agent.name)}</span>
      <div class="pm-right">${rightHtml}</div>
      ${meta}
    </div>
    ${isExpanded ? renderSubagentPanel(agent.id) : ""}`;
}

// ── timeline renderer ─────────────────────────────────────────────────────────

function tlColor(tool: string, error: boolean): string {
  if (error) return "tl-t-err";
  const t = tool.toLowerCase();
  if (t === "task")  return "tl-t-task";
  if (t === "bash")  return "tl-t-bash";
  if (t === "websearch" || t === "webfetch") return "tl-t-web";
  if (t === "read" || t === "edit" || t === "write" || t === "glob" || t === "grep") return "tl-t-file";
  return "tl-t-other";
}

function renderTimeline(): string {
  if (!_ctx) return "";
  const agents = _ctx.agents;
  if (agents.length === 0) return `<div class="pm-tl"><div class="pm-empty">No sessions</div></div>`;

  const { start: winStart, end: winEnd } = getTimeWindow();
  const winDur = winEnd - winStart;

  const ticks = tlTicks(winStart, winEnd);
  const axisHtml = ticks.map(t =>
    `<div class="pm-tl-tick" style="left:${t.pct.toFixed(1)}%">${t.label}</div>`
  ).join("");

  const nowMs  = Date.now();
  const nowPct = Math.min(100, Math.max(0, (nowMs - winStart) / winDur * 100));
  const nowLine = nowPct >= 0 && nowPct <= 100
    ? `<div class="pm-tl-now" style="left:${nowPct.toFixed(2)}%"></div>` : "";

  const sorted = [...agents].sort((a, b) => {
    const x = a as AgentState & Record<string, unknown>;
    const y = b as AgentState & Record<string, unknown>;
    return new Date(x["startedAt"] as string || a.lastActivityAt).getTime()
         - new Date(y["startedAt"] as string || b.lastActivityAt).getTime();
  });

  const lanes = sorted.map(agent => {
    const x = agent as AgentState & Record<string, unknown>;
    const sessionStart = new Date((x["startedAt"] as string) || agent.lastActivityAt).getTime();
    const sessionEnd   = new Date(agent.lastActivityAt).getTime();
    const isActive     = agent.status === "active";

    const sLeft  = Math.max(0, (sessionStart - winStart) / winDur * 100);
    const sRight = Math.min(100, (sessionEnd  - winStart) / winDur * 100);
    const sWidth = Math.max(0.3, sRight - sLeft);

    const barHtml = `<div class="pm-tl-sbar${isActive?" tl-active":""}"
      style="left:${sLeft.toFixed(2)}%;width:${sWidth.toFixed(2)}%" title="${esc(agent.name)}"></div>`;

    let blocksHtml = "";
    const subs = _tlData.get(agent.id);
    if (subs) {
      const laneEnd = [-Infinity, -Infinity];
      for (const sa of subs) {
        const ts = new Date(sa.startedAt).getTime();
        const te = sa.finishedAt ? new Date(sa.finishedAt).getTime() : ts + 2000;
        if (te < winStart || ts > winEnd) continue;
        const left  = Math.max(0, (ts - winStart) / winDur * 100);
        const right = Math.min(100, (te - winStart) / winDur * 100);
        const width = Math.max(0.2, right - left);

        const lane = ts < laneEnd[0] ? 2 : 1;
        laneEnd[lane - 1] = te;

        const col = tlColor(sa.tool, sa.error);
        const outPreview = sa.output ? `\n→ ${sa.output.replace(/\n/g," ").slice(0,80)}` : "";
        const tip  = `${sa.tool}: ${sa.prompt || "(no preview)"}${sa.durationMs ? "\n⏱ "+durFmt(sa.durationMs) : ""}${outPreview}`;
        // Click a block: zoom to the block ± half its own duration (min 10s each side)
        const blockDur  = te - ts;
        const blockPad  = Math.max(blockDur * 0.5, 10_000);
        const zoomStart = ts - blockPad;
        const zoomEnd   = te + blockPad;
        blocksHtml += `<div class="pm-tl-task ${col} lane-${lane}"
          style="left:${left.toFixed(2)}%;width:${width.toFixed(2)}%"
          data-tip="${esc(tip)}"
          data-tl-block-zoom="${zoomStart},${zoomEnd}"></div>`;
      }
    }

    const proj  = agent.projectName ? shortProject(agent.projectName) : "";
    const label = (agent.name && agent.name !== "?" ? agent.name : proj) || agent.id.slice(0, 8);
    // Click label to zoom to this session's full duration
    const sessDur = Math.max(sessionEnd - sessionStart, 30_000);
    const pad     = Math.max(sessDur * 0.1, 15_000);
    const zStart  = sessionStart - pad;
    const zEnd    = isActive ? nowMs + 2_000 : sessionEnd + pad;
    const labelHtml = `<div class="pm-tl-label${isActive?" tl-active":""}"
      data-tl-zoom="${zStart},${zEnd}" title="Click to zoom to this session">
      <span class="tl-proj">${esc(proj)}</span>
      <span class="tl-name">${esc(label.slice(0, 28))}</span>
    </div>`;

    return `<div class="pm-tl-lane">
      ${labelHtml}
      <div class="pm-tl-track">${nowLine}${barHtml}${blocksHtml}</div>
    </div>`;
  }).join("");

  const loading = _tlLoading ? `<div style="font-size:12px;color:#484f58;padding:8px 14px">Loading tool history…</div>` : "";

  // Zoom indicator bar
  const zoomBar = _tlZoom
    ? `<div class="pm-tl-zoom-bar">
        <span>Zoomed: ${fmtTickLabel(_tlZoom.start, 0)} – ${fmtTickLabel(_tlZoom.end, 0)}</span>
        <button class="pm-tl-zoom-btn" data-action="tl-zoom-reset">Reset zoom</button>
       </div>`
    : `<div class="pm-tl-zoom-bar" style="color:#21262d;font-size:10px">
        Drag track to zoom · click label or block for quick zoom
       </div>`;

  return `<div class="pm-tl">
    ${zoomBar}
    <div class="pm-tl-axis">${axisHtml}</div>
    ${lanes}
    ${loading}
  </div>`;
}

function loadTimelineData(): void {
  if (!_ctx) return;
  _tlLoading = true;
  const ids = _ctx.agents.map(a => a.id);
  let pending = ids.length;
  if (pending === 0) { _tlLoading = false; render(); return; }
  for (const id of ids) {
    if (_tlData.has(id)) { pending--; if (pending === 0) { _tlLoading = false; render(); } continue; }
    fetchSubagents(id).then(subs => {
      _tlData.set(id, subs);
      pending--;
      if (pending === 0) { _tlLoading = false; }
      render();
    }).catch(() => { pending--; if (pending === 0) { _tlLoading = false; render(); } });
  }
}

// ── dag renderer ──────────────────────────────────────────────────────────────

function renderRetroPanel(retro: any | null | undefined): string {
  // retro === undefined → still loading; null → unavailable/not analysed
  if (retro === undefined) return `<div class="pm-retro"><span class="pm-retro-loading">Loading retro analysis…</span></div>`;
  if (!retro) return "";  // not yet analysed or endpoint not available

  const j = retro.judgment;
  const s = retro.session;
  const narr = retro.narrative;
  if (!j && !s) return "";

  const outcome = j?.outcome ?? "unknown";
  const badgeClass = (outcome === "success" || outcome.startsWith("success")) ? "o-success"
    : (outcome === "failure" || outcome.startsWith("fail")) ? "o-failure"
    : (outcome === "partial" || outcome.includes("partial")) ? "o-partial" : "o-unknown";
  const outcomeLabel = outcome.includes("partial") ? "Partial"
    : outcome === "success" ? "Success"
    : outcome === "failure" ? "Failure"
    : outcome.charAt(0).toUpperCase() + outcome.replace(/_/g, " ").slice(1);

  // Summary text: prefer narrative, fall back to outcome_reasoning
  const summary = narr?.narrative ?? j?.outcome_reasoning ?? "";

  // Scores: convergence (good=high), drift (bad=high), thrash (bad=high)
  const conv  = s?.convergence_score ?? 0;
  const drift = s?.drift_score       ?? 0;
  const thrash= s?.thrash_score      ?? 0;
  const hasScores = conv > 0 || drift > 0 || thrash > 0;

  // Quality bars
  const clarity = j?.prompt_clarity    ?? 0;
  const complete= j?.prompt_completeness ?? 0;
  const hasQuality = clarity > 0 || complete > 0;

  const bar = (val: number, color: string) =>
    `<div class="pm-retro-score-t"><div class="pm-retro-score-f" style="width:${(val*100).toFixed(0)}%;background:${color}"></div></div>`;

  let scoresHtml = "";
  if (hasScores || hasQuality) {
    scoresHtml = `<div class="pm-retro-scores">`;
    if (hasQuality) {
      scoresHtml += `
        <div class="pm-retro-score"><div class="pm-retro-score-l">Clarity ${(clarity*100).toFixed(0)}%</div>${bar(clarity,"#3fb950")}</div>
        <div class="pm-retro-score"><div class="pm-retro-score-l">Completeness ${(complete*100).toFixed(0)}%</div>${bar(complete,"#58a6ff")}</div>`;
    }
    if (hasScores) {
      scoresHtml += `
        <div class="pm-retro-score"><div class="pm-retro-score-l">Convergence ${(conv*100).toFixed(0)}%</div>${bar(conv,"#3fb950")}</div>
        <div class="pm-retro-score"><div class="pm-retro-score-l">Drift ${(drift*100).toFixed(0)}%</div>${bar(drift,"#f85149")}</div>`;
    }
    scoresHtml += `</div>`;
  }

  return `<div class="pm-retro">
    <div class="pm-retro-hdr">Retro Analysis</div>
    <span class="pm-retro-badge ${badgeClass}">${outcomeLabel}</span>
    ${summary ? `<div class="pm-retro-narrative">${esc(summary.slice(0, 240))}${summary.length > 240 ? "…" : ""}</div>` : ""}
    ${scoresHtml}
  </div>`;
}

function renderStatsPanel(subs: SubAgent[], row: AgentRow | undefined, retro: any | null | undefined): string {
  const totalCalls = subs.length;
  const errorCount = subs.filter(s => s.error).length;
  const turnCount  = row?.turnCount  ?? 0;
  const costUsd    = row?.costUsd;
  const ctxPct     = row?.ctxPct;

  // Duration from tool call timestamps
  const starts = subs.map(s => new Date(s.startedAt).getTime());
  const ends   = subs.filter(s => s.finishedAt).map(s => new Date(s.finishedAt!).getTime());
  const sessionDurMs = starts.length && ends.length
    ? Math.max(...ends) - Math.min(...starts) : null;

  // Tool breakdown
  const byTool = new Map<string, { count: number; errors: number; totalMs: number }>();
  for (const sa of subs) {
    const s = byTool.get(sa.tool) ?? { count: 0, errors: 0, totalMs: 0 };
    s.count++;
    if (sa.error) s.errors++;
    if (sa.durationMs) s.totalMs += sa.durationMs;
    byTool.set(sa.tool, s);
  }
  const maxCount = Math.max(...Array.from(byTool.values()).map(s => s.count), 1);

  const toolRows = [...byTool.entries()]
    .sort((a, b) => b[1].count - a[1].count)
    .map(([tool, s]) => {
      const col  = tlColor(tool, false);
      const avgMs = s.count > 0 && s.totalMs > 0 ? s.totalMs / s.count : null;
      const pct  = (s.count / maxCount * 100).toFixed(0);
      return `<tr>
        <td><span class="pm-tl-task ${col}" style="display:inline-block;width:7px;height:7px;border-radius:50%;vertical-align:middle;margin-right:5px;flex-shrink:0"></span>${esc(tool)}</td>
        <td style="text-align:right">${s.count}</td>
        <td style="text-align:right">${s.errors > 0 ? `<span class="c-stuck">${s.errors}</span>` : "0"}</td>
        <td style="text-align:right">${s.totalMs > 0 ? durFmt(s.totalMs) : "—"}</td>
        <td style="text-align:right">${avgMs ? durFmt(avgMs) : "—"}</td>
        <td><div class="pm-st-bar-bg"><div class="pm-st-bar-fg" style="width:${pct}%"></div></div></td>
      </tr>`;
    }).join("");

  // Recent errors (last 5)
  const errors = subs.filter(s => s.error).slice(-5).reverse();
  const errorHtml = errors.length > 0
    ? errors.map(s => `<div class="pm-st-err-row">
        <span class="pm-st-err-tool">${esc(s.tool)}</span>
        <span class="pm-st-err-prompt">${esc((s.prompt || "").slice(0, 80))}</span>
        ${s.output ? `<div class="pm-st-err-out">→ ${esc(s.output.slice(0, 120))}</div>` : ""}
      </div>`).join("")
    : `<div style="color:#30363d;font-size:12px">No errors</div>`;

  const nFmt = (n: number | null | undefined, suffix = "") =>
    n != null ? `${n}${suffix}` : "—";

  return `
    ${renderRetroPanel(retro)}
    <div class="pm-st-kvrow">
      <div class="pm-st-kv"><div class="pm-st-n">${totalCalls}</div><div class="pm-st-l">tool calls</div></div>
      <div class="pm-st-kv"><div class="pm-st-n">${turnCount || "—"}</div><div class="pm-st-l">turns</div></div>
      <div class="pm-st-kv"><div class="pm-st-n${errorCount > 0 ? " c-stuck" : " c-active"}">${errorCount}</div><div class="pm-st-l">errors</div></div>
      ${costUsd != null ? `<div class="pm-st-kv"><div class="pm-st-n">$${costUsd.toFixed(3)}</div><div class="pm-st-l">cost</div></div>` : ""}
      ${sessionDurMs != null ? `<div class="pm-st-kv"><div class="pm-st-n">${durFmt(sessionDurMs)}</div><div class="pm-st-l">active time</div></div>` : ""}
      ${ctxPct != null && ctxPct > 0 ? `<div class="pm-st-kv"><div class="pm-st-n">${nFmt(ctxPct, "%")}</div><div class="pm-st-l">ctx used</div></div>` : ""}
    </div>
    <div class="pm-st-hdr">Tool Usage</div>
    <table class="pm-st-tbl">
      <thead><tr><th>Tool</th><th style="text-align:right">Calls</th><th style="text-align:right">Err</th><th style="text-align:right">Total</th><th style="text-align:right">Avg</th><th></th></tr></thead>
      <tbody>${toolRows || `<tr><td colspan="6" style="color:#30363d">No tool calls recorded</td></tr>`}</tbody>
    </table>
    <div class="pm-st-hdr">Recent Errors</div>
    <div class="pm-st-err">${errorHtml}</div>`;
}

function renderStats(): string {
  if (!_ctx) return "";
  const agents = _ctx.agents;
  if (agents.length === 0)
    return `<div class="pm-dag"><div class="pm-dag-empty">No sessions</div></div>`;

  // Auto-select first active session if nothing selected
  if (!_dagSel || !agents.find(a => a.id === _dagSel)) {
    const active = agents.find(a => a.status === "active") ?? agents[0];
    _dagSel = active?.id ?? null;
  }

  // Sidebar: session list sorted active first
  const sorted = [...agents].sort((a, b) => {
    const aA = a.status === "active" ? 0 : 1;
    const bA = b.status === "active" ? 0 : 1;
    if (aA !== bA) return aA - bA;
    return new Date(b.lastActivityAt).getTime() - new Date(a.lastActivityAt).getTime();
  });

  const sidebarItems = sorted.map(agent => {
    const row = _rows.get(agent.id);
    const isActive = agent.status === "active" ||
      (row?.toolStartedAt != null && Date.now() - row.toolStartedAt >= STUCK_MS);
    const proj = agent.projectName ? shortProject(agent.projectName) : "";
    const name = proj || agent.name || agent.id.slice(0, 12);
    const dotClass = isActive ? "s-active" : "s-idle";
    const sel = agent.id === _dagSel ? " selected" : "";
    return `<div class="pm-dag-sess${sel}" data-id="${esc(agent.id)}" data-action="dag-select">
      <span class="pm-dot ${dotClass}" style="width:6px;height:6px;flex-shrink:0"></span>
      <span class="pm-dag-sess-name${isActive?" active":""}" title="${esc(name)}">${esc(name.slice(0,22))}</span>
    </div>`;
  }).join("");

  // Main panel
  let mainHtml = "";
  if (_dagSel) {
    const subs = _tlData.get(_dagSel);
    if (subs === undefined) {
      // Trigger on-demand fetch
      if (!_statsLoading.has(_dagSel)) {
        _statsLoading.add(_dagSel);
        const id = _dagSel;
        fetchSubagents(id)
          .then(data => { _tlData.set(id, data); render(); })
          .catch(() => {})
          .finally(() => _statsLoading.delete(id));
      }
      mainHtml = `<div class="pm-dag-empty">Loading…</div>`;
    } else {
      // Fetch retro data on-demand
      const id = _dagSel;
      if (!_retroData.has(id) && !_retroLoading.has(id)) {
        _retroLoading.add(id);
        fetchRetroSession(id)
          .then(data => { _retroData.set(id, data); render(); })
          .finally(() => _retroLoading.delete(id));
      }
      const retro = _retroLoading.has(_dagSel) ? undefined : (_retroData.get(_dagSel) ?? null);
      mainHtml = renderStatsPanel(subs, _rows.get(_dagSel), retro);
    }
  }

  return `<div class="pm-dag">
    <div class="pm-dag-sidebar">${sidebarItems}</div>
    <div class="pm-dag-main">${mainHtml}</div>
  </div>`;
}

// ── main render ───────────────────────────────────────────────────────────────

function render(): void {
  if (!_ctx || !_root) return;
  const agents  = _ctx.agents;
  const now     = Date.now();
  const stuckN  = Array.from(_rows.values()).filter(r => r.toolStartedAt !== null && now - r.toolStartedAt >= STUCK_MS).length;
  const activeN = agents.filter(a => a.status === "active").length;
  // In monitor mode, count only what's visible (active + recently-done within 10 min)
  const RECENT_HEADER_MS = 10 * 60 * 1000;
  const visibleN = _mode === "list"
    ? (() => {
        const flat = buildFlat();
        return flat.filter(n => {
          const r = n.row;
          const isStuck2 = r.toolStartedAt !== null && now - r.toolStartedAt >= STUCK_MS;
          const isActive2 = r.agent.status === "active" || isStuck2;
          return isActive2 || (now - new Date(r.agent.lastActivityAt).getTime() < RECENT_HEADER_MS);
        }).length;
      })()
    : agents.length;

  const hdr = `<div class="pm-hdr">
    <span class="pm-hdr-title">Agents</span>
    <div class="pm-stat"><span class="pm-stat-n">${visibleN}</span><span class="pm-stat-l">sessions</span></div>
    ${activeN > 0 ? `<div class="pm-stat"><span class="pm-stat-n c-active">${activeN}</span><span class="pm-stat-l">active</span></div>` : ""}
    ${stuckN  > 0 ? `<div class="pm-stat"><span class="pm-stat-n c-stuck">${stuckN}</span><span class="pm-stat-l">stuck</span></div>` : ""}
    <div class="pm-tabs">
      <button class="pm-tab${_mode==="list"?" active":""}" data-mode="list">Monitor</button>
      <button class="pm-tab${_mode==="timeline"?" active":""}" data-mode="timeline">Timeline</button>
      <button class="pm-tab${_mode==="dag"?" active":""}" data-mode="dag">Stats</button>
    </div>
  </div>`;

  let body: string;
  if (_mode === "timeline") {
    body = renderTimeline();
  } else if (_mode === "dag") {
    body = renderStats();
  } else {
    // Monitor: active/stuck first, then recently-done (dimmed), hide long-idle
    const flat = buildFlat();
    const now2 = Date.now();
    const RECENT_MS = 10 * 60 * 1000; // show completed sessions for 10 min after last activity
    const active   = flat.filter(n => {
      const r = n.row;
      const isStuck2 = r.toolStartedAt !== null && now2 - r.toolStartedAt >= STUCK_MS;
      return r.agent.status === "active" || isStuck2;
    });
    const recent   = flat.filter(n => {
      const r = n.row;
      const isStuck2 = r.toolStartedAt !== null && now2 - r.toolStartedAt >= STUCK_MS;
      const isActive2 = r.agent.status === "active" || isStuck2;
      if (isActive2) return false;
      return now2 - new Date(r.agent.lastActivityAt).getTime() < RECENT_MS;
    });

    if (flat.length === 0) {
      body = `<div class="pm-empty">No agents connected</div>`;
    } else if (active.length === 0 && recent.length === 0) {
      body = `<div class="pm-empty">No active agents — switch to DAG or Timeline to browse history</div>`;
    } else {
      const clamped  = Math.min(_focusIdx, flat.length - 1);
      const activeHtml = active.length > 0
        ? active.map(n => renderRow(n, flat.indexOf(n) === clamped)).join("")
        : "";
      const divider = active.length > 0 && recent.length > 0
        ? `<div style="padding:4px 14px;font-size:11px;color:#30363d;border-top:1px solid #161b22;margin-top:2px">Recent</div>`
        : "";
      const recentHtml = recent.length > 0
        ? recent.map(n => renderRow(n, flat.indexOf(n) === clamped)).join("")
        : "";
      body = `<div class="pm-list">${activeHtml}${divider}${recentHtml}</div>`;
    }
  }

  // Save scroll positions before clobbering the DOM
  const scrollRoot    = _root.scrollTop;
  const scrollSidebar = (_root.querySelector(".pm-dag-sidebar") as HTMLElement | null)?.scrollTop ?? 0;
  const scrollMain    = (_root.querySelector(".pm-dag-main")    as HTMLElement | null)?.scrollTop ?? 0;

  _root.innerHTML = hdr + body;

  // Restore scroll positions
  _root.scrollTop = scrollRoot;
  const newSidebar = _root.querySelector(".pm-dag-sidebar") as HTMLElement | null;
  const newMain    = _root.querySelector(".pm-dag-main")    as HTMLElement | null;
  if (newSidebar) newSidebar.scrollTop = scrollSidebar;
  if (newMain)    newMain.scrollTop    = scrollMain;

  // Wire tab buttons
  _root.querySelectorAll(".pm-tab[data-mode]").forEach(el => {
    (el as HTMLElement).addEventListener("click", () => {
      const m = (el as HTMLElement).dataset["mode"] as 'list' | 'timeline' | 'dag';
      if (_mode !== m) {
        _mode = m;
        if (m === "timeline") loadTimelineData();
        else if (m === "dag") {
          // renderStats() will auto-select and trigger fetch as needed
          render();
        } else render();
      }
    });
  });

  // Wire expand buttons (list mode)
  _root.querySelectorAll("[data-action='expand']").forEach(el => {
    (el as HTMLElement).addEventListener("click", () => {
      const id = (el as HTMLElement).dataset["id"];
      if (id) toggleExpand(id);
    });
  });

  // Wire dag-select buttons (sidebar session list in Stats mode)
  _root.querySelectorAll("[data-action='dag-select']").forEach(el => {
    (el as HTMLElement).addEventListener("click", () => {
      const id = (el as HTMLElement).dataset["id"];
      if (!id || id === _dagSel) return;
      _dagSel = id;
      render(); // renderStats() triggers fetch if needed
    });
  });

  // Wire timeline zoom – session label click
  _root.querySelectorAll("[data-tl-zoom]").forEach(el => {
    (el as HTMLElement).addEventListener("click", () => {
      const val = (el as HTMLElement).dataset["tlZoom"] || "";
      const [s, e] = val.split(",").map(Number);
      if (s && e && e > s) { _tlZoom = { start: s, end: e }; render(); }
    });
  });

  // Wire timeline zoom – tool block click
  _root.querySelectorAll("[data-tl-block-zoom]").forEach(el => {
    (el as HTMLElement).addEventListener("click", (ev) => {
      ev.stopPropagation();
      const val = (el as HTMLElement).dataset["tlBlockZoom"] || "";
      const [s, e] = val.split(",").map(Number);
      if (s && e && e > s) { _tlZoom = { start: s, end: e }; render(); }
    });
  });

  // Wire zoom reset button
  const resetBtn = _root.querySelector("[data-action='tl-zoom-reset']");
  if (resetBtn) resetBtn.addEventListener("click", () => { _tlZoom = null; render(); });

  // Wire timeline drag-to-zoom (brush select)
  if (_mode === "timeline") {
    const firstTrack = _root.querySelector(".pm-tl-track") as HTMLElement | null;
    const tlEl       = _root.querySelector(".pm-tl")       as HTMLElement | null;
    if (firstTrack && tlEl) {
      tlEl.addEventListener("mousedown", (e) => {
        const me = e as MouseEvent;
        // Don't intercept clicks on labels, zoom bar, or axis
        if ((me.target as Element).closest(".pm-tl-label, .pm-tl-zoom-bar, .pm-tl-axis")) return;

        const trackRect = firstTrack.getBoundingClientRect();
        const relX = me.clientX - trackRect.left;
        if (relX < 0 || relX > trackRect.width) return;

        const tlRect = tlEl.getBoundingClientRect();

        // Create fixed-position selection rect (survives DOM re-renders)
        if (_tlSelDiv) _tlSelDiv.remove();
        _tlSelDiv = document.createElement("div");
        _tlSelDiv.className = "pm-tl-sel";
        _tlSelDiv.style.top    = `${tlRect.top}px`;
        _tlSelDiv.style.height = `${tlRect.height}px`;
        _tlSelDiv.style.left   = `${me.clientX}px`;
        _tlSelDiv.style.width  = "0px";
        document.body.appendChild(_tlSelDiv);

        const drag = { trackRect, startX: relX };

        const onMove = (e2: MouseEvent) => {
          e2.preventDefault();
          if (!_tlSelDiv) return;
          const curX = Math.max(0, Math.min(e2.clientX - drag.trackRect.left, drag.trackRect.width));
          const x1 = Math.min(drag.startX, curX);
          const x2 = Math.max(drag.startX, curX);
          _tlSelDiv.style.left  = `${drag.trackRect.left + x1}px`;
          _tlSelDiv.style.width = `${x2 - x1}px`;
        };

        const onUp = (e3: MouseEvent) => {
          document.removeEventListener("mousemove", onMove);
          document.removeEventListener("mouseup",   onUp);
          document.body.style.cursor     = "";
          document.body.style.userSelect = "";
          if (_tlSelDiv) { _tlSelDiv.remove(); _tlSelDiv = null; }

          const curX = Math.max(0, Math.min(e3.clientX - drag.trackRect.left, drag.trackRect.width));
          const x1 = Math.min(drag.startX, curX);
          const x2 = Math.max(drag.startX, curX);
          if (x2 - x1 < 5) return; // too small — treat as click, let block handlers run

          const { start: winStart, end: winEnd } = getTimeWindow();
          const winDur = winEnd - winStart;
          const tStart = winStart + (x1 / drag.trackRect.width) * winDur;
          const tEnd   = winStart + (x2 / drag.trackRect.width) * winDur;
          if (tEnd > tStart + 500) { _tlZoom = { start: tStart, end: tEnd }; render(); }
        };

        document.addEventListener("mousemove", onMove);
        document.addEventListener("mouseup",   onUp);
        document.body.style.cursor     = "ew-resize";
        document.body.style.userSelect = "none";
      });
    }
  }

  // Wire timeline tooltips
  _root.querySelectorAll("[data-tip]").forEach(el => {
    const htmlEl = el as HTMLElement;
    htmlEl.addEventListener("mouseenter", (e) => {
      if (!_tip) return;
      _tip.textContent = htmlEl.dataset["tip"] || "";
      _tip.style.display = "block";
      moveTip(e as MouseEvent);
    });
    htmlEl.addEventListener("mousemove", (e) => moveTip(e as MouseEvent));
    htmlEl.addEventListener("mouseleave", () => { if (_tip) _tip.style.display = "none"; });
  });
}

function moveTip(e: MouseEvent): void {
  if (!_tip) return;
  const x = e.clientX + 12, y = e.clientY - 8;
  _tip.style.left = `${x}px`;
  _tip.style.top  = `${y}px`;
}

// ── view ──────────────────────────────────────────────────────────────────────

const ProcessMonitorView: View = {
  id: "process-monitor", name: "Process Monitor",
  description: "Agent tree with live tool duration, timeline, and call tree views",

  mount(ctx: ViewContext): void {
    _ctx = ctx;

    _style = document.createElement("style");
    _style.textContent = CSS;
    ctx.container.appendChild(_style);

    _root = document.createElement("div");
    _root.className = "pm";
    ctx.container.appendChild(_root);

    _tip = document.createElement("div");
    _tip.className = "pm-tl-tip";
    document.body.appendChild(_tip);

    for (const a of ctx.agents) {
      _rows.set(a.id, rowFromAgent(a));
      if (a.status === "active") {
        _expanded.set(a.id, null);
        fetchSubagents(a.id).then(ag => { _expanded.set(a.id, ag); render(); }).catch(() => {});
      }
    }

    _unsubs.push(ctx.on("agent_created", (e: AgentEvent) => {
      const a = ctx.agents.find(a => a.id === e.agentId);
      if (!a) return;
      _rows.set(a.id, rowFromAgent(a));
      if (a.status === "active") {
        _expanded.set(a.id, null);
        fetchSubagents(a.id).then(ag => { _expanded.set(a.id, ag); render(); }).catch(() => {});
      }
      if (_mode === "timeline" || _mode === "dag") { _tlData.delete(a.id); loadTimelineData(); }
    }));
    _unsubs.push(ctx.on("subagent_created", (e: AgentEvent) => {
      const a = ctx.agents.find(a => a.id === e.agentId);
      if (a) _rows.set(a.id, rowFromAgent(a));
    }));
    _unsubs.push(ctx.on("agent_removed", (e: AgentEvent) => {
      _rows.delete(e.agentId); _expanded.delete(e.agentId);
      _tlData.delete(e.agentId); _statsLoading.delete(e.agentId);
      if (_dagSel === e.agentId) _dagSel = null;
    }));
    _unsubs.push(ctx.on("subagent_removed", (e: AgentEvent) => {
      _rows.delete(e.agentId); _expanded.delete(e.agentId);
    }));
    _unsubs.push(ctx.on("tool_start", (e: AgentEvent) => {
      const row = getOrCreate(e.agentId);
      if (row) row.toolStartedAt = new Date(e.timestamp).getTime();
      syncRow(e.agentId);
    }));
    _unsubs.push(ctx.on("tool_done", (e: AgentEvent) => {
      const row = getOrCreate(e.agentId);
      if (row) { if (e.payload?.["is_error"] as boolean) row.errorCount++; row.toolStartedAt = null; }
      syncRow(e.agentId);
      // Refresh expanded list and caches
      if (_expanded.has(e.agentId)) {
        _expanded.set(e.agentId, null);
        fetchSubagents(e.agentId).then(ag => { _expanded.set(e.agentId, ag); render(); }).catch(() => {});
      }
      if (_mode === "timeline" || _mode === "dag") { _tlData.delete(e.agentId); loadTimelineData(); }
    }));
    _unsubs.push(ctx.on("status_changed", (e: AgentEvent) => { syncRow(e.agentId); }));
    _unsubs.push(ctx.on("turn_complete",  (e: AgentEvent) => { syncRow(e.agentId); }));

    function onKey(e: KeyboardEvent): void {
      if (!_ctx || _mode !== "list") return;
      const flat = buildFlat();
      if (flat.length === 0) return;
      if (e.key === "j" || e.key === "ArrowDown") {
        e.preventDefault(); _focusIdx = Math.min(_focusIdx + 1, flat.length - 1);
        if (_focusIdx < 0) _focusIdx = 0; render();
      } else if (e.key === "k" || e.key === "ArrowUp") {
        e.preventDefault(); _focusIdx = Math.max(_focusIdx - 1, 0); render();
      } else if (e.key === "Enter" && _focusIdx >= 0) {
        const node = flat[_focusIdx]; if (node) toggleExpand(node.row.agent.id);
      } else if (e.key === "o" && _focusIdx >= 0) {
        const node = flat[_focusIdx];
        if (node && _ctx) _ctx.emit({ type: "focus_agent", agentId: node.row.agent.id });
      }
    }
    document.addEventListener("keydown", onKey);
    _unsubs.push(() => document.removeEventListener("keydown", onKey));

    render();
    _tick   = setInterval(render, TICK_MS);
    // Periodically refresh tool data for timeline/tree real-time updates
    _tlTick = setInterval(() => {
      if (_mode === "timeline" || _mode === "dag") {
        if (!_ctx) return;
        // Only clear and re-fetch active session data
        for (const a of _ctx.agents) {
          if (a.status === "active") _tlData.delete(a.id);
        }
        loadTimelineData();
      }
    }, TL_REFRESH_MS);
  },

  unmount(): void {
    for (const fn of _unsubs) fn();
    _unsubs = [];
    if (_tick   !== null) { clearInterval(_tick);   _tick   = null; }
    if (_tlTick !== null) { clearInterval(_tlTick); _tlTick = null; }
    if (_root)  { _root.remove();  _root  = null; }
    if (_style) { _style.remove(); _style = null; }
    if (_tip)      { _tip.remove();      _tip      = null; }
    if (_tlSelDiv) { _tlSelDiv.remove(); _tlSelDiv = null; }
    _rows = new Map(); _expanded = new Map(); _tlData = new Map();
    _statsLoading = new Set(); _retroData = new Map(); _retroLoading = new Set(); _dagSel = null;
    _focusIdx = -1; _tlLoading = false; _mode = "list"; _tlZoom = null; _ctx = null;
  },
};

export default ProcessMonitorView;
