"use client";

import { useCallback, useSyncExternalStore } from "react";
import { Monitor, Moon, Sun } from "lucide-react";

type Theme = "light" | "dark" | "system";

const ORDER: Theme[] = ["system", "light", "dark"];
const ICONS = { system: Monitor, light: Sun, dark: Moon };
const LABELS = {
  system: "Theme: follows your system",
  light: "Theme: light",
  dark: "Theme: dark",
};

const STORAGE_KEY = "uil-theme";
const EVENT = "uil-theme-change";

/**
 * localStorage is an external store, so it is read through
 * useSyncExternalStore rather than an effect. The server snapshot is always
 * "system", which matches the pre-paint script in layout.tsx.
 */
function subscribe(onChange: () => void) {
  window.addEventListener(EVENT, onChange);
  window.addEventListener("storage", onChange);
  return () => {
    window.removeEventListener(EVENT, onChange);
    window.removeEventListener("storage", onChange);
  };
}

function getSnapshot(): Theme {
  const stored = localStorage.getItem(STORAGE_KEY);
  return stored === "dark" || stored === "light" ? stored : "system";
}

function getServerSnapshot(): Theme {
  return "system";
}

export function ThemeToggle() {
  const theme = useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);

  const cycle = useCallback(() => {
    const next = ORDER[(ORDER.indexOf(theme) + 1) % ORDER.length];
    if (next === "system") {
      localStorage.removeItem(STORAGE_KEY);
      document.documentElement.removeAttribute("data-theme");
    } else {
      localStorage.setItem(STORAGE_KEY, next);
      document.documentElement.setAttribute("data-theme", next);
    }
    window.dispatchEvent(new Event(EVENT));
  }, [theme]);

  const Icon = ICONS[theme];

  return (
    <button
      type="button"
      onClick={cycle}
      title={LABELS[theme]}
      aria-label={LABELS[theme]}
      className="grid h-11 w-11 sm:h-9 sm:w-9 place-items-center rounded-lg border transition hover:border-[var(--muted)]"
      style={{ background: "var(--surface)" }}
    >
      <Icon size={16} strokeWidth={2} aria-hidden />
    </button>
  );
}
