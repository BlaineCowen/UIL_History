import type { Metadata, Viewport } from "next";
import Link from "next/link";
import { ThemeToggle } from "@/components/ThemeToggle";
import { NavLinks } from "@/components/NavLinks";
import "./globals.css";

export const metadata: Metadata = {
  title: {
    default: "UIL History — Texas Concert & Sight-Reading results",
    template: "%s · UIL History",
  },
  description:
    "Explore two decades of Texas UIL Concert & Sight-Reading contest results and the Prescribed Music List.",
};

/**
 * Matches --page in globals.css, so the mobile browser chrome blends into the
 * page instead of banding against it. Deliberately no maximumScale/
 * userScalable: pinch-zoom stays available.
 */
export const viewport: Viewport = {
  themeColor: [
    { media: "(prefers-color-scheme: light)", color: "#f9f9f7" },
    { media: "(prefers-color-scheme: dark)", color: "#0d0d0d" },
  ],
};

/**
 * Applied before paint so a dark-mode visitor never sees a white flash.
 * Mirrors the CSS in globals.css: an explicit choice wins over the OS setting.
 */
const themeScript = `
(function () {
  try {
    var stored = localStorage.getItem("uil-theme");
    if (stored === "dark" || stored === "light") {
      document.documentElement.setAttribute("data-theme", stored);
    }
  } catch (e) {}
})();
`;

export default function RootLayout({
  children,
}: Readonly<{ children: React.ReactNode }>) {
  return (
    <html lang="en" suppressHydrationWarning>
      <head>
        <script dangerouslySetInnerHTML={{ __html: themeScript }} />
      </head>
      <body className="min-h-screen flex flex-col">
        <header className="sticky top-0 z-30 border-b backdrop-blur-md bg-[color-mix(in_srgb,var(--page)_88%,transparent)]">
          <div className="mx-auto w-full max-w-[1400px] px-4 sm:px-6">
            <div className="flex h-14 items-center gap-2 sm:gap-6">
              <Link
                href="/"
                aria-label="UIL History — home"
                className="tap flex items-center gap-2 shrink-0 font-semibold tracking-tight"
              >
                <span
                  aria-hidden
                  className="grid h-7 w-7 place-items-center rounded-md text-[13px] font-bold text-white"
                  style={{ background: "var(--series-1)" }}
                >
                  U
                </span>
                <span className="hidden sm:inline">UIL History</span>
              </Link>

              <NavLinks />

              <div className="ml-auto flex items-center gap-2">
                <ThemeToggle />
              </div>
            </div>
          </div>
        </header>

        <main className="flex-1 mx-auto w-full max-w-[1400px] px-4 sm:px-6 py-5 sm:py-8">
          {children}
        </main>

        <footer className="border-t mt-8 pb-[env(safe-area-inset-bottom)]">
          <div className="mx-auto w-full max-w-[1400px] px-4 sm:px-6 py-6 flex flex-col sm:flex-row sm:items-center gap-4 justify-between">
            <p className="text-sm" style={{ color: "var(--ink-2)" }}>
              Built by{" "}
              <a
                className="underline underline-offset-2 hover:no-underline"
                href="mailto:blaine.cowen@gmail.com"
              >
                Blaine Cowen
              </a>
              . Data scraped from texasmusicforms.com.
            </p>
            <a
              href="https://www.buymeacoffee.com/blainecowen"
              target="_blank"
              rel="noreferrer noopener"
              className="tap inline-flex items-center justify-center gap-2 rounded-lg border px-3 py-2 text-sm font-medium transition hover:border-[var(--muted)] self-start sm:self-auto"
              style={{ background: "var(--surface)" }}
            >
              ☕ Buy me a coffee
            </a>
          </div>
        </footer>
      </body>
    </html>
  );
}
