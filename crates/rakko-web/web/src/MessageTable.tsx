import { useEffect, useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { Summary } from "./api";
import { formatBytes, formatTime } from "./format";

const ROW_HEIGHT = 30;

type Props = {
  rows: Summary[];
  selected: Summary | null;
  onSelect: (s: Summary) => void;
  /** Show a trailing "scanning…" row (more rows are still coming). */
  loading?: boolean;
  /** Called with the index of the last visible row as the user scrolls. */
  onVisibleEnd?: (index: number) => void;
  /** Scrolls back to the top whenever this changes (new filter / new topic). */
  resetKey?: unknown;
  empty?: string;
};

/** Virtualized message rows, shared by the scan view and the live tail. */
export function MessageTable({ rows, selected, onSelect, loading, onVisibleEnd, resetKey, empty }: Props) {
  const scroller = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length + (loading ? 1 : 0),
    getScrollElement: () => scroller.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 14,
  });
  const items = virtualizer.getVirtualItems();
  const lastVisible = items.length > 0 ? items[items.length - 1].index : 0;

  useEffect(() => {
    onVisibleEnd?.(lastVisible);
  }, [lastVisible, rows.length, onVisibleEnd]);

  useEffect(() => {
    scroller.current?.scrollTo({ top: 0 });
  }, [resetKey]);

  return (
    <div className="table">
      <div className="row head">
        <span>time</span>
        <span>p</span>
        <span>offset</span>
        <span>key</span>
        <span>value</span>
        <span>fmt</span>
      </div>
      <div className="scroller" ref={scroller}>
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {items.map((item) => {
            const row = rows[item.index];
            const style = { position: "absolute" as const, top: 0, left: 0, width: "100%", height: item.size, transform: `translateY(${item.start}px)` };
            if (!row) {
              return (
                <div key="loading" className="row loading" style={style}>
                  scanning…
                </div>
              );
            }
            const isSelected = selected?.partition === row.partition && selected.offset === row.offset;
            return (
              <div
                key={`${row.partition}:${row.offset}`}
                className={`row${isSelected ? " selected" : ""}`}
                style={style}
                onClick={() => onSelect(row)}
              >
                <span className="dim">{formatTime(row.timestamp_ms)}</span>
                <span>{row.partition}</span>
                <span>{row.offset}</span>
                <span className="clip" title={row.key.preview}>
                  {row.key.preview}
                </span>
                <span className="clip" title={`${formatBytes(row.value.size)}`}>
                  {row.value.preview}
                </span>
                <span className="fmt">{row.value.format}</span>
              </div>
            );
          })}
        </div>
        {!loading && rows.length === 0 && empty && <div className="empty">{empty}</div>}
      </div>
    </div>
  );
}
