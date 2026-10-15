import { useMemo, useState } from "react";
import type { LagSample } from "./api";
import { formatCount } from "./format";

const W = 900;
const H = 220;
const PAD = { l: 56, r: 12, t: 12, b: 26 };
const COLORS = ["#a78bfa", "#67d4e0", "#e5c07b", "#7fd18b", "#f0707a", "#d19a66", "#61afef", "#c678dd"];
const RANGES: { label: string; ms: number }[] = [
  { label: "15 min", ms: 15 * 60_000 },
  { label: "1 h", ms: 60 * 60_000 },
  { label: "6 h", ms: 6 * 60 * 60_000 },
  { label: "24 h", ms: 24 * 60 * 60_000 },
];

const clock = (ms: number) => new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });

/** Consumer-group lag over time: the total, plus one line per topic when there are several. */
export function LagChart({ samples, intervalS }: { samples: LagSample[]; intervalS: number }) {
  const [range, setRange] = useState(RANGES[1].ms);
  const [hover, setHover] = useState<number | null>(null);

  const view = useMemo(() => {
    const end = samples.length ? samples[samples.length - 1].t : Date.now();
    const start = end - range;
    const shown = samples.filter((s) => s.t >= start);
    const topics = [...new Set(shown.flatMap((s) => Object.keys(s.topics)))].sort();
    const lines = topics.length > 1 ? [{ name: "total", get: (s: LagSample) => s.total }, ...topics.map((t) => ({ name: t, get: (s: LagSample) => s.topics[t] ?? 0 }))] : [{ name: "total", get: (s: LagSample) => s.total }];
    const max = Math.max(1, ...shown.map((s) => s.total));
    return { shown, lines, max, start, end };
  }, [samples, range]);

  const x = (t: number) => PAD.l + ((t - view.start) / Math.max(1, view.end - view.start)) * (W - PAD.l - PAD.r);
  const y = (v: number) => PAD.t + (1 - v / view.max) * (H - PAD.t - PAD.b);
  const path = (get: (s: LagSample) => number) => view.shown.map((s, i) => `${i ? "L" : "M"}${x(s.t).toFixed(1)},${y(get(s)).toFixed(1)}`).join(" ");
  const ticks = [0, 0.5, 1].map((f) => Math.round(view.max * f));
  const at = hover !== null ? view.shown[hover] : null;

  return (
    <section className="lag-chart">
      <header>
        <h3 className="section">Lag over time</h3>
        <div className="seg">
          {RANGES.map((r) => (
            <button key={r.label} className={range === r.ms ? "on" : undefined} onClick={() => setRange(r.ms)}>
              {r.label}
            </button>
          ))}
        </div>
      </header>
      {view.shown.length < 2 ? (
        <div className="dim pad">
          Collecting samples (one every {intervalS}s while this group is open; history is kept in the server's memory from when it was first viewed).
        </div>
      ) : (
        <>
          <svg
            viewBox={`0 0 ${W} ${H}`}
            role="img"
            aria-label="consumer group lag over time"
            onMouseLeave={() => setHover(null)}
            onMouseMove={(e) => {
              const box = e.currentTarget.getBoundingClientRect();
              const px = ((e.clientX - box.left) / box.width) * W;
              const t = view.start + ((px - PAD.l) / (W - PAD.l - PAD.r)) * (view.end - view.start);
              let best = 0;
              view.shown.forEach((s, i) => {
                if (Math.abs(s.t - t) < Math.abs(view.shown[best].t - t)) best = i;
              });
              setHover(best);
            }}
          >
            {ticks.map((v) => (
              <g key={v}>
                <line x1={PAD.l} x2={W - PAD.r} y1={y(v)} y2={y(v)} className="grid-line" />
                <text x={PAD.l - 6} y={y(v) + 4} textAnchor="end" className="axis">
                  {formatCount(v)}
                </text>
              </g>
            ))}
            <text x={PAD.l} y={H - 6} className="axis">
              {clock(view.start)}
            </text>
            <text x={W - PAD.r} y={H - 6} textAnchor="end" className="axis">
              {clock(view.end)}
            </text>
            {view.lines.map((l, i) => (
              <path key={l.name} d={path(l.get)} fill="none" stroke={COLORS[i % COLORS.length]} strokeWidth={i === 0 ? 2.2 : 1.4} opacity={i === 0 ? 1 : 0.85} />
            ))}
            {at && <line x1={x(at.t)} x2={x(at.t)} y1={PAD.t} y2={H - PAD.b} className="cursor-line" />}
          </svg>
          <div className="legend">
            {view.lines.map((l, i) => (
              <span key={l.name}>
                <i style={{ background: COLORS[i % COLORS.length] }} />
                {l.name}
                {at ? `: ${formatCount(l.get(at))}` : ""}
              </span>
            ))}
            {at && <span className="dim">{new Date(at.t).toLocaleString()}</span>}
          </div>
        </>
      )}
    </section>
  );
}
