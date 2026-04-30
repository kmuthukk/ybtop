# Changelog

All notable functional changes to **ybtop** are listed here by release. Format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) (newest first).

## [0.1.7]

### Added

- **`table_id` in snapshots:** `yb_local_tablets` rows include `table_id`; ASH rows resolved via `yb_local_tablets` include `table_id` so table vs index / duplicate names across schemas are distinguishable.
- **Browser ASH drill-down:** `node_id` links open ASH scoped by `node`; `object_name` links (when `table_id` exists) open ASH scoped by `table_id`. URL parameters `node` and `table_id` compose with other filters as documented in the viewer.
- **ASH by query (table-scoped):** When filtering by `table_id`, a roll-up by **`query_id`** shows which statements drive activity against that table/index.
- **`query_id` deeplinks** across major ASH tables in the viewer (consistent with pg_stat statement links).

### Changed

- **Python `merge_ash_groups`:** Merge key prefers **`table_id`** when present (aligned with browser grouping).
- **Viewer ASH layout:** Redundant sections are omitted under node / table / query scopes where appropriate; **“ASH samples by database”** is always the **last** ASH subsection.

## [0.1.6]

### Changed

- **Tablet distribution (browser):** Counts and breakdowns use only tablets in **`TABLET_DATA_READY`** state; clearer messaging when raw tablet rows exist but none qualify.

## [0.1.5]

### Added

- **SQL tagging:** Outgoing queries can be prefixed with **`/* service:ybtop */`** for identification in server logs (via shared DB tagging helpers).

### Changed

- **`ybtop watch`:** Long statement text in the live dashboard is **truncated** to a short preview (multi-line SQL summarized).
- **ASH rollups (browser + `merge_ash_groups`):** Grouping no longer splits solely on different **`wait_event_aux`** when rows share the same **object / tablet identity**, reducing duplicate “same object” lines.

## [0.1.4]

### Added

- **`ybtop watch` live dashboard:** Alternate-screen layout with merged **top pg_stat_statements**, **nodes ranked by ASH active sessions/sec**, and **ASH summarized by cloud / region / zone**.
- **Delta pg_stat in watch:** When an older snapshot exists in the manifest, the statements panel can show **Δ vs prior snapshot**.
- **Manifest / snapshot helpers** to load prior snapshots for delta and viewer-related flows.

### Changed

- **Embedded HTTP viewer:** Bind happens **before** watch starts; bind or output-directory failures **exit with status 1** instead of continuing without a working viewer.
- **Live layout:** Snapshot write errors surface inside the dashboard; **`Live`** does not redirect stdout/stderr (prints are not swallowed).
- **Terminal UX:** Viewer URL uses **OSC 8** without Rich-specific link IDs where relevant for broader terminal compatibility; a **first-checkpoint collecting** message appears before the initial snapshot completes.

## [0.1.3]

### Added

- Initial **ybtop** release: **pg_stat_statements**, **ASH**, and **tablet** collection into JSON snapshots, CLI **`watch`** / **`serve`**, and static **browser viewer**.
