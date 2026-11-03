import { ImageResponse } from "next/og";

/**
 * The preview card shown when a link is pasted into Facebook, iMessage, Slack,
 * Discord, etc. Without one the layout's summary_large_image card renders as a
 * bare text link, which gets far fewer clicks in a director's group chat.
 *
 * Inherited by every route below app/, so song pages share it too. It reads no
 * data, so Next renders it once at build time -- a database outage cannot
 * break it, and it never blocks a deploy.
 */
export const alt = "UIL History — Texas UIL Prescribed Music List and contest results";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

const BLUE = "#2a78d6";

export default function Image() {
  return new ImageResponse(
    (
      <div
        style={{
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          justifyContent: "space-between",
          padding: 72,
          background: "#f9f9f7",
          color: "#111",
          fontFamily: "sans-serif",
        }}
      >
        <div style={{ display: "flex", alignItems: "center", gap: 24 }}>
          {/* Same mark as the header and favicon. */}
          <svg width="88" height="88" viewBox="0 0 32 32">
            <rect width="32" height="32" rx="7" fill={BLUE} />
            <path
              d="M10 8v9a6 6 0 0 0 12 0V8"
              fill="none"
              stroke="#fff"
              strokeWidth="4"
              strokeLinecap="round"
            />
          </svg>
          <div style={{ fontSize: 44, fontWeight: 700, letterSpacing: -1 }}>UIL History</div>
        </div>

        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          <div style={{ fontSize: 72, fontWeight: 700, lineHeight: 1.05, letterSpacing: -2 }}>
            Texas UIL Prescribed Music List
          </div>
          <div style={{ fontSize: 34, color: "#555", lineHeight: 1.3 }}>
            Every PML piece, how often it&apos;s been played at Concert &amp; Sight-Reading,
            and how those performances scored.
          </div>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 16, fontSize: 30 }}>
          <div style={{ width: 14, height: 14, borderRadius: 7, background: BLUE }} />
          <div style={{ color: BLUE, fontWeight: 700 }}>uilpml.com</div>
          <div style={{ color: "#777" }}>· Band · Choir · Orchestra</div>
        </div>
      </div>
    ),
    size,
  );
}
