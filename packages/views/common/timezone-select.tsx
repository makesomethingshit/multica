"use client";

import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@multica/ui/components/ui/select";

// IANA 2022b: Europe/Kiev is a backward-compat alias for Europe/Kyiv.
function canConstructTimezone(tz: string): boolean {
  try {
    new Intl.DateTimeFormat("en", { timeZone: tz });
    return true;
  } catch {
    return false;
  }
}

function normalizeTimezone(tz: string): string {
  if (tz === "Europe/Kiev" && canConstructTimezone("Europe/Kyiv")) return "Europe/Kyiv";
  return tz;
}

// Curated fallback list used when the runtime lacks `Intl.supportedValuesOf`.
// Exported so every timezone picker draws from one source instead of
// drifting copies.

export const COMMON_TIMEZONES = [
  "UTC",
  "America/Los_Angeles",
  "America/Denver",
  "America/Chicago",
  "America/New_York",
  "America/Sao_Paulo",
  "Europe/London",
  "Europe/Berlin",
  "Europe/Paris",
  "Europe/Moscow",
  "Europe/Kyiv",
  "Africa/Cairo",
  "Asia/Dubai",
  "Asia/Kolkata",
  "Asia/Bangkok",
  "Asia/Shanghai",
  "Asia/Singapore",
  "Asia/Tokyo",
  "Australia/Sydney",
  "Pacific/Auckland",
];

let cachedBrowserTZ: string | null = null;
export function browserTimezone(): string {
  if (cachedBrowserTZ !== null) return cachedBrowserTZ;
  try {
    cachedBrowserTZ = normalizeTimezone(Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC");
  } catch {
    cachedBrowserTZ = "UTC";
  }
  return cachedBrowserTZ;
}

// Clears the module-level browserTimezone() cache. Browser code never
// needs this — the tz is stable for a session — but the cache survives
// across Vitest files in the same worker, so any test that stubs
// `Intl.DateTimeFormat` (directly or via a fake timezone) MUST call this
// in `beforeEach`, otherwise a value cached by an earlier suite leaks in.
// Tests that mock the whole `./timezone-select` module are unaffected.
export function resetBrowserTimezoneCache(): void {
  cachedBrowserTZ = null;
}

type IntlWithSupportedValues = typeof Intl & {
  supportedValuesOf?: (key: "timeZone") => string[];
};

function supportedTimezones(): string[] {
  try {
    const supported = (Intl as IntlWithSupportedValues).supportedValuesOf?.(
      "timeZone",
    );
    return (supported && supported.length > 0 ? supported : COMMON_TIMEZONES).map(normalizeTimezone);
  } catch {
    return COMMON_TIMEZONES.map(normalizeTimezone);
  }
}

export function timezoneOptions(current: string): string[] {
  const browser = browserTimezone();
  return Array.from(new Set([current, browser, ...COMMON_TIMEZONES, ...supportedTimezones()].map((tz) => (tz ? normalizeTimezone(tz) : tz)).filter(Boolean)));
}

export function TimezoneSelect({
  value,
  onValueChange,
  browserSuffix,
  disabled,
  triggerClassName,
}: {
  value: string;
  onValueChange: (next: string) => void;
  browserSuffix: string;
  disabled?: boolean;
  triggerClassName?: string;
}) {
  const browser = browserTimezone();
  const options = timezoneOptions(value);
  const render = (tz: string) =>
    tz === browser ? `${tz}${browserSuffix}` : tz;
  const items = options.map((value) => ({ value, label: render(value) }));

  return (
    <Select
      items={items}
      value={value}
      disabled={disabled}
      onValueChange={(next) => {
        if (next) onValueChange(next);
      }}
    >
      <SelectTrigger
        size="sm"
        className={triggerClassName ?? "w-full rounded-md font-mono text-caption"}
      >
        <SelectValue>{render(value)}</SelectValue>
      </SelectTrigger>
      <SelectContent align="start" className="max-h-72">
        {items.map((item) => (
          <SelectItem key={item.value} value={item.value} className="font-mono text-caption">
            {item.label}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}
