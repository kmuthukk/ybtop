/* global fetch, document, window, navigator */

(function () {
  const MANIFEST = "ybtop.manifest.json";

  /** Optional DocDB columns on pg_stat_statements (YugabyteDB); keep in sync with pg_stat_constants.py */
  const PG_STAT_DOCDB_KEYS = [
    "docdb_seeks",
    "docdb_nexts",
    "docdb_prevs",
    "docdb_read_rpcs",
    "docdb_write_rpcs",
    "catalog_wait_time",
    "docdb_read_operations",
    "docdb_write_operations",
    "docdb_rows_scanned",
    "docdb_rows_returned",
    "docdb_wait_time",
    "conflict_retries",
    "read_restart_retries",
    "total_retries",
    "docdb_obsolete_rows_scanned",
    "docdb_read_time",
    "docdb_write_time",
  ];

  const CLIPBOARD_SVG =
    '<svg xmlns="http://www.w3.org/2000/svg" width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>';

  let manifestEntries = [];
  let currentIndex = -1;
  let lastDoc = null;
  /** Prior snapshot (for delta pg_stat), retained for re-rend on tab/URL. */
  let lastPrevDoc = null;

  /** Left nav panel id; survives snapshot navigation. */
  let activeViewerSection = "pgss";
  /** ASH `query_id` filter from URL or pg_stat link; cleared when leaving the ASH tab. */
  let ashQueryIdFilter = null;

  const VIEWER_SECTION_IDS = ["pgss", "ash", "tablets"];

  /**
   * subsectionId -> expanded when true; undefined / false => collapsed.
   * `sec-pgss-main` defaults to expanded so the statements table and pager are visible.
   * State survives snapshot Prev/Next.
   */
  const subsectionExpandedState = Object.create(null);

  function isSubsectionExpanded(subsectionId) {
    if (subsectionId === "sec-pgss-main" && subsectionExpandedState[subsectionId] === undefined) {
      return true; /* pg_stat table + page controls visible on first open */
    }
    return subsectionExpandedState[subsectionId] === true;
  }

  function setSubsectionExpanded(subsectionId, expanded) {
    subsectionExpandedState[subsectionId] = !!expanded;
  }

  function wireSubsectionCollapse(section, subsectionId, bodyEl, toggleBtn) {
    function sync() {
      const open = isSubsectionExpanded(subsectionId);
      bodyEl.hidden = !open;
      toggleBtn.textContent = open ? "▼" : "▶";
      toggleBtn.setAttribute("aria-expanded", open ? "true" : "false");
      toggleBtn.setAttribute("aria-label", open ? "Collapse section" : "Expand section");
      section.classList.toggle("subsection-expanded", open);
    }
    sync();
    toggleBtn.addEventListener("click", () => {
      setSubsectionExpanded(subsectionId, !isSubsectionExpanded(subsectionId));
      sync();
    });
  }

  function readViewerStateFromUrl() {
    const p = new URLSearchParams(window.location.search);
    const v = p.get("view");
    if (v === "ash" || v === "tablets" || v === "pgss") {
      activeViewerSection = v;
    } else {
      activeViewerSection = "pgss";
    }
    const q = p.get("query");
    ashQueryIdFilter = q != null && String(q) !== "" ? String(q) : null;
    if (activeViewerSection !== "ash") {
      ashQueryIdFilter = null;
    }
  }

  function writeViewerStateToUrl(options) {
    const push = options && options.push;
    const p = new URLSearchParams();
    p.set("view", activeViewerSection);
    if (activeViewerSection === "ash" && ashQueryIdFilter) {
      p.set("query", ashQueryIdFilter);
    }
    const qs = p.toString();
    const newUrl = `${window.location.pathname}${qs ? "?" + qs : ""}${window.location.hash || ""}`;
    const st = { ybtop: true, view: activeViewerSection, query: ashQueryIdFilter || null };
    if (push) {
      history.pushState(st, "", newUrl);
    } else {
      history.replaceState(st, "", newUrl);
    }
  }

  function setViewerSection(id) {
    if (!VIEWER_SECTION_IDS.includes(id)) return;
    const hadFilter = !!ashQueryIdFilter;
    if (id !== "ash") {
      ashQueryIdFilter = null;
    }
    activeViewerSection = id;
    const app = document.getElementById("app");
    if (!app) return;
    app.querySelectorAll(".app-panel").forEach((p) => {
      const on = p.dataset.viewerSection === id;
      p.classList.toggle("app-panel-active", on);
      p.setAttribute("aria-hidden", on ? "false" : "true");
    });
    const nav = document.getElementById("app-nav");
    if (nav) {
      nav.querySelectorAll(".app-tab").forEach((b) => {
        const on = b.dataset.viewerSection === id;
        b.classList.toggle("app-tab-active", on);
        b.setAttribute("aria-selected", on ? "true" : "false");
      });
    }
    writeViewerStateToUrl();
    if (lastDoc && id !== "ash" && hadFilter) {
      renderDoc(lastDoc, lastPrevDoc);
    } else {
      updateAshFilterToolbar();
    }
  }

  function buildViewerNav() {
    const nav = document.getElementById("app-nav");
    if (!nav) return;
    if (!VIEWER_SECTION_IDS.includes(activeViewerSection)) {
      activeViewerSection = "pgss";
    }
    nav.textContent = "";
    const items = [
      ["pgss", "pg_stat_statements"],
      ["ash", "Active Session History"],
      ["tablets", "Tablet Report"],
    ];
    items.forEach(([sid, label]) => {
      const btn = el("button", {
        type: "button",
        className: "app-tab",
        textContent: label,
        "data-viewer-section": sid,
        role: "tab",
        id: `tab-${sid}`,
        "aria-controls": `panel-${sid}`,
      });
      btn.addEventListener("click", () => setViewerSection(sid));
      nav.appendChild(btn);
    });
    setViewerSection(activeViewerSection);
  }

  function el(tag, attrs, children) {
    const n = document.createElement(tag);
    if (attrs) {
      Object.entries(attrs).forEach(([k, v]) => {
        if (k === "className") n.className = v;
        else if (k === "textContent") n.textContent = v;
        else if (k === "innerHTML") n.innerHTML = v;
        else n.setAttribute(k, v);
      });
    }
    (children || []).forEach((c) => n.appendChild(c));
    return n;
  }

  function normQid(v) {
    if (v === null || v === undefined) return null;
    return String(v);
  }

  function mergeStatements(perNode) {
    let hasRowsInSource = false;
    let hasDbnameInSource = false;
    Object.keys(perNode || {}).forEach((nid) => {
      (perNode[nid] || []).forEach((r) => {
        if (Object.prototype.hasOwnProperty.call(r, "rows")) hasRowsInSource = true;
        const dbv = r.dbname;
        if (dbv != null && dbv !== undefined && String(dbv).trim() !== "") hasDbnameInSource = true;
      });
    });

    const seenDoc = new Set();
    Object.keys(perNode || {}).forEach((nid) => {
      (perNode[nid] || []).forEach((r) => {
        PG_STAT_DOCDB_KEYS.forEach((k) => {
          if (r[k] != null && r[k] !== undefined) seenDoc.add(k);
        });
      });
    });
    const docKeys = PG_STAT_DOCDB_KEYS.filter((k) => seenDoc.has(k));

    const acc = new Map();
    Object.keys(perNode || {}).forEach((nid) => {
      (perNode[nid] || []).forEach((r) => {
        const dn = r.dbname != null && r.dbname !== undefined ? String(r.dbname).trim() : "";
        const mk = `${String(r.queryid)}\0${dn}`;
        if (!acc.has(mk)) {
          const o = {
            queryid: String(r.queryid),
            dbname: dn || null,
            query: r.query || "",
            calls: 0,
            total_exec_time: 0,
          };
          if (hasRowsInSource) o.rows = 0;
          docKeys.forEach((k) => {
            o[k] = 0;
          });
          acc.set(mk, o);
        }
        const a = acc.get(mk);
        a.calls += Number(r.calls) || 0;
        a.total_exec_time += Number(r.total_exec_time) || 0;
        if (!a.dbname && r.dbname) a.dbname = String(r.dbname).trim() || null;
        if (hasRowsInSource) a.rows += Number(r.rows) || 0;
        docKeys.forEach((k) => {
          a[k] += Number(r[k]) || 0;
        });
        if (!a.query && r.query) a.query = r.query;
      });
    });
    const out = Array.from(acc.values()).map((a) => {
      const calls = a.calls;
      const mean = calls ? a.total_exec_time / calls : 0;
      const row = {
        calls: a.calls,
        total_ms: Math.round(a.total_exec_time * 100) / 100,
        mean_ms: Math.round(mean * 100) / 100,
        query: a.query,
      };
      if (hasDbnameInSource) {
        row.dbname = a.dbname != null ? a.dbname : null;
      }
      if (hasRowsInSource) {
        row.rows = Math.round(a.rows * 100) / 100;
        row.rows_per_call = calls ? Math.round((a.rows / calls) * 100) / 100 : 0;
      }
      docKeys.forEach((k) => {
        row[`${k}_per_call`] = calls ? Math.round((a[k] / calls) * 100) / 100 : 0;
      });
      row.queryid = a.queryid;
      const deltaSrc = {
        calls: a.calls,
        total_exec_time: a.total_exec_time,
        doc: {},
      };
      if (hasRowsInSource) deltaSrc.rows = a.rows;
      docKeys.forEach((k) => {
        deltaSrc.doc[k] = a[k];
      });
      row._deltaSrc = deltaSrc;
      return row;
    });
    out.sort((x, y) => y.total_ms - x.total_ms);
    return out;
  }

  function pgStatStatementColumns(merged, perNode) {
    let hasRowsInSource = false;
    let hasDbnameInSource = false;
    Object.keys(perNode || {}).forEach((nid) => {
      (perNode[nid] || []).forEach((r) => {
        if (Object.prototype.hasOwnProperty.call(r, "rows")) hasRowsInSource = true;
        if (Object.prototype.hasOwnProperty.call(r, "dbname")) hasDbnameInSource = true;
      });
    });
    const cols = [
      { key: "calls", label: "calls", type: "number" },
      { key: "total_ms", label: "time (ms)", type: "number" },
      { key: "time_pct", label: "time %", type: "number" },
      { key: "mean_ms", label: "mean_ms", type: "number" },
      { key: "query", label: "query" },
    ];
    if (hasDbnameInSource) {
      cols.push({ key: "dbname", label: "dbname" });
    }
    if (hasRowsInSource) {
      cols.push({
        key: "rows_per_call",
        label: "rows per call",
        type: "number",
        headerPerCall: true,
        headerBase: "rows",
        sortValue: (r) => {
          const x =
            r.rows_per_call != null && r.rows_per_call !== ""
              ? r.rows_per_call
              : r.avg_rows_per_call;
          return Number(x) || 0;
        },
      });
    }
    PG_STAT_DOCDB_KEYS.forEach((k) => {
      const kk = `${k}_per_call`;
      if (merged.some((r) => Object.prototype.hasOwnProperty.call(r, kk))) {
        cols.push({ key: kk, type: "number", headerPerCall: true, headerBase: k });
      }
    });
    cols.push({ key: "queryid", label: "queryid" });
    return cols;
  }

  function statementMergeKey(r) {
    const dn = r.dbname != null && r.dbname !== undefined ? String(r.dbname).trim() : "";
    return `${String(r.queryid)}\0${dn}`;
  }

  /** Reconstruct approximate raw totals when _deltaSrc is missing (older snapshots). */
  function deltaSrcFromRowFallback(r) {
    if (!r) return { calls: 0, total_exec_time: 0, rows: 0, doc: {} };
    if (r._deltaSrc) return r._deltaSrc;
    const calls = Number(r.calls) || 0;
    const doc = {};
    PG_STAT_DOCDB_KEYS.forEach((k) => {
      const pk = `${k}_per_call`;
      if (!Object.prototype.hasOwnProperty.call(r, pk)) return;
      const pc = Number(r[pk]) || 0;
      doc[k] = calls * pc;
    });
    return {
      calls,
      total_exec_time: Number(r.total_ms) || 0,
      rows: r.rows != null ? Number(r.rows) : undefined,
      doc,
    };
  }

  /**
   * Per-statement deltas: new snapshot merged row minus previous (same queryid+dbname).
   * mean_ms = (Δ total_exec_time) / (Δ calls); DocDB and rows per-call use Δtotals / Δcalls.
   */
  function deltaPgStatMergedRows(curRows, prevRows) {
    const prevMap = new Map();
    (prevRows || []).forEach((r) => {
      prevMap.set(statementMergeKey(r), r);
    });
    const raw = [];
    (curRows || []).forEach((cur) => {
      const p = prevMap.get(statementMergeKey(cur)) || null;
      const sc = deltaSrcFromRowFallback(cur);
      const sp = p ? deltaSrcFromRowFallback(p) : { calls: 0, total_exec_time: 0, rows: 0, doc: {} };
      const dCalls = sc.calls - (sp.calls || 0);
      const dExec = sc.total_exec_time - (sp.total_exec_time || 0);
      const hasRows = Object.prototype.hasOwnProperty.call(cur, "rows");
      const dRows = hasRows ? (Number(sc.rows) || 0) - (sp.rows != null ? Number(sp.rows) || 0 : 0) : 0;
      const docKeySet = new Set();
      PG_STAT_DOCDB_KEYS.forEach((k) => {
        if ((sc.doc && k in sc.doc) || (sp.doc && k in sp.doc)) docKeySet.add(k);
        if (Object.prototype.hasOwnProperty.call(cur, `${k}_per_call`)) docKeySet.add(k);
        if (p && Object.prototype.hasOwnProperty.call(p, `${k}_per_call`)) docKeySet.add(k);
      });
      const row = {
        calls: Math.round(dCalls * 100) / 100,
        total_ms: Math.round(dExec * 100) / 100,
        mean_ms: dCalls > 0 ? Math.round((dExec / dCalls) * 100) / 100 : 0,
        query: cur.query,
        queryid: cur.queryid,
      };
      if (Object.prototype.hasOwnProperty.call(cur, "dbname")) {
        row.dbname = cur.dbname != null ? cur.dbname : null;
      }
      if (hasRows) {
        row.rows = Math.round(dRows * 100) / 100;
        row.rows_per_call = dCalls > 0 ? Math.round((dRows / dCalls) * 100) / 100 : 0;
      }
      docKeySet.forEach((dk) => {
        const ctot = sc.doc && sc.doc[dk] != null ? Number(sc.doc[dk]) : 0;
        const ptot = sp.doc && sp.doc[dk] != null ? Number(sp.doc[dk]) : 0;
        const dtot = ctot - ptot;
        row[`${dk}_per_call`] = dCalls > 0 ? Math.round((dtot / dCalls) * 100) / 100 : 0;
      });
      raw.push(row);
    });
    const filtered = raw.filter((r) => {
      if (r.calls !== 0 || r.total_ms !== 0) return true;
      if (r.rows != null && r.rows !== 0) return true;
      return PG_STAT_DOCDB_KEYS.some(
        (k) =>
          Object.prototype.hasOwnProperty.call(r, `${k}_per_call`) && Number(r[`${k}_per_call`]) !== 0
      );
    });
    filtered.sort((a, b) => b.total_ms - a.total_ms);
    return filtered;
  }

  function pgStatStatementColumnsDelta(merged, perNode) {
    let hasRowsInSource = false;
    let hasDbnameInSource = false;
    Object.keys(perNode || {}).forEach((nid) => {
      (perNode[nid] || []).forEach((r) => {
        if (Object.prototype.hasOwnProperty.call(r, "rows")) hasRowsInSource = true;
        if (Object.prototype.hasOwnProperty.call(r, "dbname")) hasDbnameInSource = true;
      });
    });
    const cols = [
      { key: "calls_per_sec", label: "calls/s", type: "number" },
      { key: "total_ms", label: "time (ms)", type: "number" },
      { key: "time_pct", label: "time %", type: "number" },
      { key: "mean_ms", label: "mean_ms", type: "number" },
      { key: "query", label: "query" },
    ];
    if (hasDbnameInSource) {
      cols.push({ key: "dbname", label: "dbname" });
    }
    if (hasRowsInSource) {
      cols.push({
        key: "rows_per_call",
        label: "rows per call",
        type: "number",
        headerPerCall: true,
        headerBase: "rows",
        sortValue: (r) => {
          const x =
            r.rows_per_call != null && r.rows_per_call !== ""
              ? r.rows_per_call
              : r.avg_rows_per_call;
          return Number(x) || 0;
        },
      });
    }
    PG_STAT_DOCDB_KEYS.forEach((k) => {
      const kk = `${k}_per_call`;
      if (merged.some((r) => Object.prototype.hasOwnProperty.call(r, kk))) {
        cols.push({ key: kk, type: "number", headerPerCall: true, headerBase: k });
      }
    });
    cols.push({ key: "queryid", label: "queryid" });
    return cols;
  }

  /** Fixed-width UTC timestamp for activity headers (23 chars). */
  function formatSnapshotTsFixed(iso) {
    if (iso == null || iso === "") return "????-??-?? ??:??:?? UTC";
    try {
      const d = new Date(String(iso));
      if (Number.isNaN(d.getTime())) return String(iso).slice(0, 23).padEnd(23);
      const y = d.getUTCFullYear();
      const mo = String(d.getUTCMonth() + 1).padStart(2, "0");
      const da = String(d.getUTCDate()).padStart(2, "0");
      const h = String(d.getUTCHours()).padStart(2, "0");
      const mi = String(d.getUTCMinutes()).padStart(2, "0");
      const s = String(d.getUTCSeconds()).padStart(2, "0");
      return `${y}-${mo}-${da} ${h}:${mi}:${s} UTC`;
    } catch {
      return "????-??-?? ??:??:?? UTC";
    }
  }

  /** Positive span in seconds between older and newer snapshot timestamps (for ratio math). */
  function snapshotIntervalSeconds(olderIso, newerIso) {
    const t1 = new Date(String(olderIso)).getTime();
    const t2 = new Date(String(newerIso)).getTime();
    if (Number.isNaN(t1) || Number.isNaN(t2) || t2 <= t1) return 0;
    return (t2 - t1) / 1000;
  }

  /** Cumulative mode: time % = row total_ms / sum(total_ms) over displayed rows. */
  function withPgStatTimePercent(rows) {
    const arr = rows || [];
    const totalMs = arr.reduce((s, r) => s + (Number(r.total_ms) || 0), 0);
    return arr.map((r) => {
      const ms = Number(r.total_ms) || 0;
      return {
        ...r,
        time_pct: totalMs > 0 ? Math.round(10000 * (ms / totalMs)) / 100 : 0,
      };
    });
  }

  /**
   * Delta-mode derived fields: calls/s = Δcalls / interval, time % = row Δ total_ms / sum(Δ total_ms).
   */
  function withPgStatDeltaDerivedRows(rows, olderIso, newerIso) {
    const sec = snapshotIntervalSeconds(olderIso, newerIso);
    const arr = rows || [];
    const totalMs = arr.reduce((s, r) => s + (Number(r.total_ms) || 0), 0);
    return arr.map((r) => {
      const calls = Number(r.calls) || 0;
      const ms = Number(r.total_ms) || 0;
      return {
        ...r,
        calls_per_sec: sec > 0 ? Math.round((calls / sec) * 100) / 100 : 0,
        time_pct: totalMs > 0 ? Math.round(10000 * (ms / totalMs)) / 100 : 0,
      };
    });
  }

  /**
   * Terse human-readable span between two snapshot timestamps, e.g. "14s", "1min", "2h 15min", "1d 3h".
   */
  function formatDurationHuman(iso1, iso2) {
    const t1 = new Date(String(iso1)).getTime();
    const t2 = new Date(String(iso2)).getTime();
    if (Number.isNaN(t1) || Number.isNaN(t2)) return "—";
    let ms = t2 - t1;
    if (ms < 0) ms = 0;
    let sec = Math.floor(ms / 1000);
    if (sec === 0) return "0s";

    const days = Math.floor(sec / 86400);
    sec -= days * 86400;
    const hours = Math.floor(sec / 3600);
    sec -= hours * 3600;
    const mins = Math.floor(sec / 60);
    const secs = sec - mins * 60;

    const parts = [];
    if (days) parts.push(`${days}d`);
    if (hours) parts.push(`${hours}h`);
    if (mins) parts.push(`${mins}min`);
    if (secs) parts.push(`${secs}s`);
    return parts.join(" ");
  }

  function pgStatActivityBannerAt(tsIso) {
    const wrap = el("div", { className: "pgss-activity-banner" });
    const strong = el("strong", { className: "pgss-activity-title" });
    strong.appendChild(document.createTextNode("Activity @ "));
    const ts = el("span", { className: "yb-mono pgss-activity-mono" });
    ts.textContent = formatSnapshotTsFixed(tsIso);
    strong.appendChild(ts);
    wrap.appendChild(strong);
    return wrap;
  }

  function pgStatActivityBannerDelta(iso1, iso2) {
    const wrap = el("div", { className: "pgss-activity-banner" });
    const strong = el("strong", { className: "pgss-activity-title" });
    strong.appendChild(document.createTextNode("Activity for "));
    const dur = el("span", { className: "yb-mono pgss-activity-mono" });
    dur.textContent = formatDurationHuman(iso1, iso2);
    strong.appendChild(dur);
    strong.appendChild(document.createTextNode(" from "));
    const t1 = el("span", { className: "yb-mono pgss-activity-mono" });
    t1.textContent = formatSnapshotTsFixed(iso1);
    strong.appendChild(t1);
    strong.appendChild(document.createTextNode(" to "));
    const t2 = el("span", { className: "yb-mono pgss-activity-mono" });
    t2.textContent = formatSnapshotTsFixed(iso2);
    strong.appendChild(t2);
    wrap.appendChild(strong);
    return wrap;
  }

  /** ASH: same banner layout as delta pg_stat, but the interval is the snapshot’s ash_window, not time between snapshots. */
  function ashWindowActivityBanner(doc) {
    const w = doc && doc.ash_window;
    if (
      w &&
      w.start_utc != null &&
      w.end_utc != null &&
      String(w.start_utc) !== "" &&
      String(w.end_utc) !== ""
    ) {
      return pgStatActivityBannerDelta(w.start_utc, w.end_utc);
    }
    const wrap = el("div", { className: "pgss-activity-banner" });
    wrap.appendChild(
      el("p", {
        className: "pgss-activity-note",
        textContent: "This snapshot has no ASH time window (ash_window); the ASH query interval is unknown.",
      })
    );
    return wrap;
  }

  /** YSQL + no wait_event_aux + no object_name → show object as [PGLayer]. */
  function ashDisplayObjectName(r) {
    const c = r.wait_event_component;
    const aux = r.wait_event_aux;
    const ob = r.object_name;
    const auxEmpty = aux == null || aux === "" || String(aux).trim() === "";
    const obEmpty = ob == null || ob === "" || String(ob).trim() === "";
    if (c != null && String(c).trim().toUpperCase() === "YSQL" && auxEmpty && obEmpty) {
      return "[PGLayer]";
    }
    return obEmpty ? null : String(ob);
  }

  /** Group ASH rows by displayed object identity (not wait_event_aux — many aux values share one object). */
  function ashMergeKey(r) {
    const disp = ashDisplayObjectName(r);
    const objKey = disp != null ? String(disp) : "";
    return [
      normQid(r.query_id),
      r.wait_event_component,
      r.wait_event,
      r.wait_event_type,
      objKey,
      r.ysql_dbid == null ? "" : String(r.ysql_dbid),
    ].join("\0");
  }

  function mergeAsh(perNode) {
    const merged = new Map();
    Object.keys(perNode || {}).forEach((nid) => {
      (perNode[nid] || []).forEach((r) => {
        const k = ashMergeKey(r);
        if (!merged.has(k)) {
          merged.set(k, {
            query_id: r.query_id,
            wait_event_component: r.wait_event_component,
            wait_event: r.wait_event,
            wait_event_type: r.wait_event_type,
            wait_event_aux: r.wait_event_aux,
            ysql_dbid: r.ysql_dbid != null && r.ysql_dbid !== undefined ? r.ysql_dbid : null,
            namespace_name: r.namespace_name != null ? r.namespace_name : null,
            object_name: r.object_name != null ? r.object_name : null,
            samples: 0,
            query: r.query || "",
          });
        }
        const m = merged.get(k);
        m.samples += Number(r.samples) || 0;
        if (!m.query && r.query) m.query = r.query;
        m.namespace_name = m.namespace_name || r.namespace_name || null;
        m.object_name = m.object_name || r.object_name || null;
        if (m.ysql_dbid == null && r.ysql_dbid != null && r.ysql_dbid !== undefined) {
          m.ysql_dbid = r.ysql_dbid;
        }
      });
    });
    const rows = Array.from(merged.values()).map((m) =>
      Object.assign({}, m, { object_name: ashDisplayObjectName(m) })
    );
    rows.sort((a, b) => b.samples - a.samples);
    return rows;
  }

  /**
   * Match ASH row to filter id. New snapshots use query_id as text (same as pg_stat queryid) so JS does not
   * lose 64-bit precision. For legacy JSON with query_id as a number, BigInt() compares the true integer.
   */
  function rowMatchesAshQueryIdFilter(r, wantRaw) {
    const want = String(wantRaw).trim();
    if (want === "") return false;
    const a = r.query_id != null && r.query_id !== undefined ? r.query_id : r.queryid;
    if (a == null) return false;
    if (String(a) === want) return true;
    if (typeof BigInt === "function" && /^-?\d+$/.test(want)) {
      try {
        if (BigInt(String(a)) === BigInt(want)) return true;
      } catch (e) {
        return false;
      }
    }
    return false;
  }

  function filterAshPerNodeByQueryId(perNode, qidStr) {
    const want = String(qidStr).trim();
    if (want === "") return perNode;
    const out = {};
    Object.keys(perNode || {}).forEach((nid) => {
      const rows = (perNode[nid] || []).filter((r) => rowMatchesAshQueryIdFilter(r, want));
      if (rows.length) out[nid] = rows;
    });
    return out;
  }

  function getFirstQueryTextForFilter(doc, qid) {
    if (!doc) return null;
    const raw = doc.yb_active_session_history && doc.yb_active_session_history.per_node;
    if (!raw) return null;
    const f = filterAshPerNodeByQueryId(raw, qid);
    for (const i = 0, keys = Object.keys(f); i < keys.length; i += 1) {
      const rows = f[keys[i]] || [];
      for (let j = 0; j < rows.length; j += 1) {
        if (rows[j].query) return String(rows[j].query);
      }
    }
    return null;
  }

  function getQueryTextForToolbar(doc, qid) {
    const fromAsh = getFirstQueryTextForFilter(doc, qid);
    if (fromAsh) return fromAsh;
    const want = normQid(qid);
    if (want == null) return null;
    const st = doc && doc.pg_stat_statements && doc.pg_stat_statements.per_node;
    if (!st) return null;
    for (const i = 0, keys = Object.keys(st); i < keys.length; i += 1) {
      const rows = st[keys[i]] || [];
      for (let j = 0; j < rows.length; j += 1) {
        const r = rows[j];
        if (normQid(r && r.queryid) === want && r.query) return String(r.query);
      }
    }
    return null;
  }

  function updateAshFilterToolbar() {
    /* Reserved: header no longer shows ASH filter context (details are in the ASH panel). */
  }

  /** When ASH is scoped to one query_id, table columns for query / query_id are redundant. */
  function ashColumnsWithoutQueryIdAndQuery(cols) {
    return cols.filter((c) => c.key !== "query_id" && c.key !== "query");
  }

  function buildAshQueryHref(qid) {
    const p = new URLSearchParams();
    p.set("view", "ash");
    p.set("query", String(qid));
    return `${window.location.pathname}?${p.toString()}`;
  }

  function navigateToAshForQueryId(qid) {
    const s = String(qid).trim();
    if (!s) return;
    ashQueryIdFilter = s;
    activeViewerSection = "ash";
    /* pushState so the browser Back button returns to the prior tab (e.g. statements). */
    writeViewerStateToUrl({ push: true });
    if (lastDoc) {
      renderDoc(lastDoc, lastPrevDoc);
    }
  }

  function flattenAsh(perNode, topo) {
    const out = [];
    Object.keys(perNode || {}).forEach((nid) => {
      const t = (topo && topo[nid]) || {};
      (perNode[nid] || []).forEach((r) => {
        const row = Object.assign({}, r, {
          node_id: nid,
          cloud: t.cloud || "",
          region: t.region || "",
          zone: t.zone || "",
        });
        row.object_name = ashDisplayObjectName(row);
        out.push(row);
      });
    });
    return out;
  }

  function groupSum(rows, keyFn) {
    const m = new Map();
    rows.forEach((r) => {
      const k = keyFn(r);
      const prev = m.get(k) || { key: k, samples: 0 };
      prev.samples += Number(r.samples) || 0;
      m.set(k, prev);
    });
    return Array.from(m.values()).sort((a, b) => b.samples - a.samples);
  }

  /** Sum samples by node_id; attach cloud/region/zone from first seen row per node (topology is per-node). */
  function sumAshByNode(rows) {
    const m = new Map();
    rows.forEach((r) => {
      const nid = r.node_id;
      const add = Number(r.samples) || 0;
      if (!m.has(nid)) {
        m.set(nid, {
          node_id: nid,
          cloud: r.cloud != null && r.cloud !== undefined ? String(r.cloud) : "",
          region: r.region != null && r.region !== undefined ? String(r.region) : "",
          zone: r.zone != null && r.zone !== undefined ? String(r.zone) : "",
          samples: 0,
        });
      }
      const ent = m.get(nid);
      ent.samples += add;
    });
    return Array.from(m.values()).sort((a, b) => b.samples - a.samples);
  }

  /** Group merged ASH rows by namespace + query; sum samples. */
  function groupAshByNamespaceQuery(rows) {
    const m = new Map();
    (rows || []).forEach((r) => {
      const nn = r.namespace_name != null && r.namespace_name !== undefined ? String(r.namespace_name) : "";
      const q = r.query != null && r.query !== undefined ? String(r.query) : "";
      const k = JSON.stringify([nn, q]);
      if (!m.has(k)) {
        m.set(k, { namespace_name: nn, query: q, samples: 0 });
      }
      m.get(k).samples += Number(r.samples) || 0;
    });
    return Array.from(m.values()).sort((a, b) => b.samples - a.samples);
  }

  /** Group merged ASH rows by namespace + object + query; sum samples. */
  function groupAshByNamespaceObjectQuery(rows) {
    const m = new Map();
    (rows || []).forEach((r) => {
      const nn = r.namespace_name != null && r.namespace_name !== undefined ? String(r.namespace_name) : "";
      const on = r.object_name != null && r.object_name !== undefined ? String(r.object_name) : "";
      const q = r.query != null && r.query !== undefined ? String(r.query) : "";
      const k = JSON.stringify([nn, on, q]);
      if (!m.has(k)) {
        m.set(k, { namespace_name: nn, object_name: on, query: q, samples: 0 });
      }
      const ent = m.get(k);
      ent.samples += Number(r.samples) || 0;
    });
    return Array.from(m.values()).sort((a, b) => b.samples - a.samples);
  }

  /**
   * ASH: add load_pct = 100 * row.samples / sum(samples over totalRows).
   * When `totalRows` is set (e.g. full set before a Top-50 slice), the denominator uses that;
   * otherwise the sum is over `pageRows` only.
   */
  function withAshLoadPercent(pageRows, totalRows) {
    const forTotal = totalRows != null && totalRows !== undefined ? totalRows : pageRows;
    const total = (forTotal || []).reduce((s, r) => s + (Number(r.samples) || 0), 0);
    return (pageRows || []).map((r) => ({
      ...r,
      load_pct: total > 0 ? Math.round(10000 * ((Number(r.samples) || 0) / total)) / 100 : 0,
    }));
  }

  /** Half-open [ash_window.start_utc, ash_window.end_utc) length in seconds; min ~1e-9 to avoid div-by-zero. */
  function ashWindowIntervalSeconds(snap) {
    const w = snap && snap.ash_window;
    if (!w) return 1;
    const t1 = new Date(String(w.start_utc || "")).getTime();
    const t2 = new Date(String(w.end_utc || "")).getTime();
    if (Number.isNaN(t1) || Number.isNaN(t2) || t2 <= t1) return 1;
    return Math.max(1e-9, (t2 - t1) / 1000);
  }

  /**
   * ASH: sessions_per_sec = samples / window_seconds (per snapshot ash_window in JSON).
   * Raw `samples` is kept for load %.
   */
  function withAshSessionsPerSec(rows, intervalSec) {
    const d = Math.max(1e-9, Number(intervalSec) || 0);
    return (rows || []).map((r) => ({
      ...r,
      sessions_per_sec: (Number(r.samples) || 0) / d,
    }));
  }

  function formatAshSessionsPerSec(n) {
    if (n == null || n === "") return "";
    const x = Number(n);
    if (Number.isNaN(x)) return String(n);
    if (x === 0) return "0";
    if (x >= 100) return x.toFixed(2);
    if (x >= 10) return x.toFixed(3);
    if (x >= 1) return x.toFixed(4);
    return x.toFixed(5);
  }

  function tabletTableKey(namespaceName, tableName) {
    const ns = namespaceName != null && namespaceName !== undefined ? String(namespaceName).trim() : "";
    const tn = tableName != null && tableName !== undefined ? String(tableName).trim() : "";
    return `${ns}\0${tn}`;
  }

  function flattenLocalTablets(perNode, topo) {
    const out = [];
    Object.keys(perNode || {}).forEach((nid) => {
      const t = (topo && topo[nid]) || {};
      (perNode[nid] || []).forEach((r) => {
        out.push(
          Object.assign({}, r, {
            node_id: nid,
            cloud: t.cloud != null && t.cloud !== undefined ? String(t.cloud) : "",
            region: t.region != null && t.region !== undefined ? String(t.region) : "",
            zone: t.zone != null && t.zone !== undefined ? String(t.zone) : "",
          })
        );
      });
    });
    return out;
  }

  /** Per logical table: total tablets and per-node counts (desc); node id only in tooltips. */
  function tabletsPerTableReport(perNode) {
    const byTable = new Map();
    Object.keys(perNode || {}).forEach((nid) => {
      (perNode[nid] || []).forEach((r) => {
        const k = tabletTableKey(r.namespace_name, r.table_name);
        if (!byTable.has(k)) byTable.set(k, new Map());
        const byNode = byTable.get(k);
        byNode.set(nid, (byNode.get(nid) || 0) + 1);
      });
    });
    const rows = [];
    byTable.forEach((byNode, k) => {
      const parts = String(k).split("\0");
      const ns = parts[0] || "";
      const tbl = parts.length > 1 ? parts.slice(1).join("\0") : "";
      let total = 0;
      byNode.forEach((c) => {
        total += c;
      });
      const perNodeCounts = Array.from(byNode.entries())
        .map(([node_id, count]) => ({ node_id, count }))
        .sort((a, b) => b.count - a.count);
      rows.push({
        namespace_name: ns,
        table_name: tbl || "(unknown)",
        tablets: total,
        per_node_counts: perNodeCounts,
      });
    });
    rows.sort((a, b) => b.tablets - a.tablets);
    return rows;
  }

  function tabletsPerNodeReport(perNode, topo) {
    const rows = Object.keys(perNode || {}).map((nid) => {
      const t = (topo && topo[nid]) || {};
      return {
        node_id: nid,
        tablets: (perNode[nid] || []).length,
        cloud: t.cloud != null && t.cloud !== undefined ? String(t.cloud) : "",
        region: t.region != null && t.region !== undefined ? String(t.region) : "",
        zone: t.zone != null && t.zone !== undefined ? String(t.zone) : "",
      };
    });
    rows.sort((a, b) => b.tablets - a.tablets);
    return rows;
  }

  /** Tablet counts grouped by placement triple from node topology. */
  function tabletsPerCloudRegionZoneReport(perNode, topo) {
    const flat = flattenLocalTablets(perNode, topo);
    const m = new Map();
    flat.forEach((r) => {
      const c = String(r.cloud || "").trim();
      const reg = String(r.region || "").trim();
      const z = String(r.zone || "").trim();
      const k = `${c}\t${reg}\t${z}`;
      m.set(k, (m.get(k) || 0) + 1);
    });
    const rows = Array.from(m.entries()).map(([key, tablets]) => {
      const p = String(key).split("\t");
      return {
        cloud: p[0] != null ? p[0] : "",
        region: p[1] != null ? p[1] : "",
        zone: p[2] != null ? p[2] : "",
        tablets,
      };
    });
    rows.sort((a, b) => b.tablets - a.tablets);
    return rows;
  }

  async function fetchJson(url) {
    const res = await fetch(url, { cache: "no-store" });
    if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
    return res.json();
  }

  async function loadManifest() {
    const raw = await fetchJson(MANIFEST);
    if (Array.isArray(raw)) return raw;
    if (raw && Array.isArray(raw.entries)) return raw.entries;
    return [];
  }

  function setStatus(msg, isErr) {
    const s = document.getElementById("status-msg");
    s.textContent = msg || "";
    s.style.color = isErr ? "var(--yb-danger)" : "var(--yb-muted)";
  }

  function copyText(text) {
    const t = String(text || "");
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(t).catch(() => fallbackCopy(t));
    }
    return fallbackCopy(t);
  }

  function fallbackCopy(t) {
    const ta = document.createElement("textarea");
    ta.value = t;
    ta.style.position = "fixed";
    ta.style.left = "-9999px";
    document.body.appendChild(ta);
    ta.select();
    try {
      document.execCommand("copy");
    } finally {
      document.body.removeChild(ta);
    }
    return Promise.resolve();
  }

  /** Table cell keys shown in a fixed-width (code) font — SQL, names, ids, wait events. */
  const MONO_TABLE_CELL_KEYS = new Set([
    "query",
    "queryid",
    "query_id",
    "namespace_name",
    "object_name",
    "namespace_objname",
    "db_name",
    "dbname",
    "relname",
    "tablet_id",
    "table_name",
    "node_id",
    "leader",
    "wait_event",
    "wait_event_component",
    "wait_event_type",
    "wait_event_aux",
    "ysql_dbid",
    "cloud",
    "region",
    "zone",
    "cloud_region_zone",
    "cloud_region",
  ]);

  function applyMonoTableCellClass(td, colKey) {
    if (colKey === "query" || MONO_TABLE_CELL_KEYS.has(colKey)) {
      td.classList.add("yb-mono");
    }
  }

  let _queryTipEl = null;
  let _queryTipShowTimer = null;
  let _queryTipHideTimer = null;
  let _queryTipGlobalWired = false;

  function hideQueryTooltipImmediate() {
    if (_queryTipShowTimer) {
      clearTimeout(_queryTipShowTimer);
      _queryTipShowTimer = null;
    }
    if (_queryTipHideTimer) {
      clearTimeout(_queryTipHideTimer);
      _queryTipHideTimer = null;
    }
    if (_queryTipEl && _queryTipEl.classList.contains("query-tooltip-popup-visible")) {
      _queryTipEl.classList.remove("query-tooltip-popup-visible");
      _queryTipEl.setAttribute("aria-hidden", "true");
    }
  }

  function ensureQueryTipDismissOnScrollResize() {
    if (_queryTipGlobalWired) {
      return;
    }
    _queryTipGlobalWired = true;
    window.addEventListener("scroll", hideQueryTooltipImmediate, true);
    window.addEventListener("resize", hideQueryTooltipImmediate);
  }

  function getQueryTooltipEl() {
    if (_queryTipEl) {
      return _queryTipEl;
    }
    ensureQueryTipDismissOnScrollResize();
    _queryTipEl = el("div", {
      className: "query-tooltip-popup",
      "aria-hidden": "true",
      role: "tooltip",
    });
    _queryTipEl.addEventListener("mouseenter", () => {
      if (_queryTipHideTimer) {
        clearTimeout(_queryTipHideTimer);
        _queryTipHideTimer = null;
      }
    });
    _queryTipEl.addEventListener("mouseleave", scheduleHideQueryTooltip);
    document.body.appendChild(_queryTipEl);
    return _queryTipEl;
  }

  function scheduleHideQueryTooltip() {
    if (_queryTipHideTimer) {
      clearTimeout(_queryTipHideTimer);
    }
    _queryTipHideTimer = setTimeout(() => {
      const tip = _queryTipEl;
      if (tip) {
        tip.classList.remove("query-tooltip-popup-visible");
        tip.setAttribute("aria-hidden", "true");
      }
      _queryTipHideTimer = null;
    }, 200);
  }

  function positionAndShowQueryTooltip(anchorRect, fullText) {
    const tip = getQueryTooltipEl();
    tip.textContent = fullText;
    tip.setAttribute("aria-hidden", "false");
    const margin = 8;
    const maxW = Math.min(window.innerWidth * 0.92, 52 * 16);
    tip.style.maxWidth = `${maxW}px`;
    let left = anchorRect.left;
    if (left + maxW > window.innerWidth - margin) {
      left = Math.max(margin, window.innerWidth - maxW - margin);
    }
    let top = anchorRect.bottom - 1;
    tip.style.left = `${left}px`;
    tip.style.top = `${top}px`;
    tip.classList.add("query-tooltip-popup-visible");
    const th = tip.offsetHeight;
    const maxH = window.innerHeight - 2 * margin;
    if (th > maxH) {
      tip.style.maxHeight = `${maxH}px`;
    } else {
      tip.style.maxHeight = "";
    }
    if (top + tip.offsetHeight > window.innerHeight - margin) {
      const up = anchorRect.top - tip.offsetHeight + 1;
      if (up >= margin) {
        tip.style.top = `${up}px`;
      } else {
        tip.style.top = `${margin}px`;
        tip.style.maxHeight = `${window.innerHeight - 2 * margin}px`;
      }
    }
  }

  function appendQueryCell(td, queryVal) {
    const full = String(queryVal || "");
    const wrap = el("div", { className: "query-cell" });
    const span = el("span", { className: "query-preview" });
    span.textContent = full;
    wrap.appendChild(span);
    const btn = el("button", {
      type: "button",
      className: "icon-copy-btn",
      "aria-label": "Copy query",
      title: "Copy query",
      innerHTML: CLIPBOARD_SVG,
    });
    btn.addEventListener("click", () => {
      copyText(queryVal).then(() => {
        btn.classList.add("icon-copy-done");
        setTimeout(() => btn.classList.remove("icon-copy-done"), 1200);
      });
    });
    wrap.appendChild(btn);
    wrap.addEventListener("mouseenter", () => {
      if (_queryTipHideTimer) {
        clearTimeout(_queryTipHideTimer);
        _queryTipHideTimer = null;
      }
      if (_queryTipShowTimer) {
        clearTimeout(_queryTipShowTimer);
      }
      _queryTipShowTimer = setTimeout(() => {
        positionAndShowQueryTooltip(wrap.getBoundingClientRect(), full);
        _queryTipShowTimer = null;
      }, 80);
    });
    wrap.addEventListener("mouseleave", () => {
      if (_queryTipShowTimer) {
        clearTimeout(_queryTipShowTimer);
        _queryTipShowTimer = null;
      }
      scheduleHideQueryTooltip();
    });
    td.appendChild(wrap);
  }

  function appendQueryCellWithAshLinks(td, queryVal, qid) {
    const full = String(queryVal || "");
    const wrap = el("div", { className: "query-cell" });
    const a = el("a", {
      className: "query-preview query-ash-deeplink",
      href: buildAshQueryHref(qid),
      textContent: full,
    });
    a.addEventListener("click", (e) => {
      e.preventDefault();
      navigateToAshForQueryId(qid);
    });
    wrap.appendChild(a);
    const btn = el("button", {
      type: "button",
      className: "icon-copy-btn",
      "aria-label": "Copy query",
      title: "Copy query",
      innerHTML: CLIPBOARD_SVG,
    });
    btn.addEventListener("click", (e) => {
      e.stopPropagation();
      copyText(queryVal).then(() => {
        btn.classList.add("icon-copy-done");
        setTimeout(() => btn.classList.remove("icon-copy-done"), 1200);
      });
    });
    wrap.appendChild(btn);
    wrap.addEventListener("mouseenter", () => {
      if (_queryTipHideTimer) {
        clearTimeout(_queryTipHideTimer);
        _queryTipHideTimer = null;
      }
      if (_queryTipShowTimer) {
        clearTimeout(_queryTipShowTimer);
      }
      _queryTipShowTimer = setTimeout(() => {
        positionAndShowQueryTooltip(wrap.getBoundingClientRect(), full);
        _queryTipShowTimer = null;
      }, 80);
    });
    wrap.addEventListener("mouseleave", () => {
      if (_queryTipShowTimer) {
        clearTimeout(_queryTipShowTimer);
        _queryTipShowTimer = null;
      }
      scheduleHideQueryTooltip();
    });
    td.appendChild(wrap);
  }

  /** Descending tablet counts only; each value's title is host:port for that node. */
  function appendTabletCountStripCell(td, pairs) {
    td.classList.add("yb-mono", "yb-wrap-cell", "yb-count-strip");
    if (!pairs || !pairs.length) {
      td.textContent = "";
      return;
    }
    pairs.forEach((p, i) => {
      if (i > 0) td.appendChild(document.createTextNode(", "));
      const span = el("span", {
        className: "yb-count-chip",
        title: String(p.node_id || ""),
      });
      span.textContent = String(p.count);
      td.appendChild(span);
    });
  }

  function appendColumnHeader(th, col, options) {
    const unify = options && options.unifyStatementHeaders;
    th.dataset.sortKey = col.key;
    th.dataset.sortType = col.type || "string";
    if (col.headerPerCall && col.headerBase) {
      th.classList.add("th-per-call-metric");
      const m = String(col.headerBase).trim();
      th.appendChild(document.createTextNode(m ? `${m} / call` : "/ call"));
    } else {
      if (unify) {
        th.classList.add("th-per-call-metric");
      }
      th.textContent = col.label != null ? String(col.label) : String(col.key);
    }
  }

  function buildSortableTable(title, rows, columns, subsectionId) {
    const section = el("section", { className: "ybtop-section" });
    const body = el("div", { className: "section-body" });
    if (subsectionId) {
      const header = el("div", { className: "section-header" });
      const toggle = el("button", { type: "button", className: "section-toggle" });
      const h2 = el("h2", { className: "section-title", textContent: title });
      header.appendChild(toggle);
      header.appendChild(h2);
      section.appendChild(header);
      wireSubsectionCollapse(section, subsectionId, body, toggle);
    } else {
      section.appendChild(el("h2", { className: "section-title", textContent: title }));
    }
    section.appendChild(body);

    if (!rows.length) {
      body.appendChild(el("p", { textContent: "(no rows)" }));
      return section;
    }
    const state = { key: columns[0].key, dir: "desc" };

    const table = el("table");
    const thead = el("thead");
    const trh = el("tr");
    columns.forEach((col) => {
      const th = el("th");
      appendColumnHeader(th, col);
      th.addEventListener("click", () => {
        if (state.key === col.key) state.dir = state.dir === "asc" ? "desc" : "asc";
        else {
          state.key = col.key;
          state.dir = col.type === "number" ? "desc" : "asc";
        }
        trh.querySelectorAll("th").forEach((x) => {
          x.classList.remove("sort-asc", "sort-desc");
        });
        th.classList.add(state.dir === "asc" ? "sort-asc" : "sort-desc");
        renderBody();
      });
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);
    const tbody = el("tbody");
    table.appendChild(tbody);

    function cmp(a, b) {
      const col = columns.find((c) => c.key === state.key) || columns[0];
      let va;
      let vb;
      if (typeof col.sortValue === "function") {
        va = col.sortValue(a);
        vb = col.sortValue(b);
      } else {
        va = a[state.key];
        vb = b[state.key];
      }
      if (col.type === "number") {
        va = Number(va) || 0;
        vb = Number(vb) || 0;
      } else {
        va = String(va || "").toLowerCase();
        vb = String(vb || "").toLowerCase();
      }
      if (va < vb) return state.dir === "asc" ? -1 : 1;
      if (va > vb) return state.dir === "asc" ? 1 : -1;
      return 0;
    }

    function renderBody() {
      tbody.textContent = "";
      const sorted = rows.slice().sort(cmp);
      sorted.forEach((row) => {
        const tr = el("tr");
        columns.forEach((col) => {
          const td = el("td");
          const v = row[col.key];
          if (col.key === "query") {
            applyMonoTableCellClass(td, col.key);
            appendQueryCell(td, v);
          } else if (col.key === "per_node_counts") {
            appendTabletCountStripCell(td, v);
          } else if (col.key === "load_pct" || col.key === "time_pct") {
            applyMonoTableCellClass(td, col.key);
            td.textContent =
              v === null || v === undefined || v === "" ? "" : `${Number(v).toFixed(2)}%`;
          } else if (col.key === "calls_per_sec") {
            applyMonoTableCellClass(td, col.key);
            td.textContent =
              v === null || v === undefined || v === "" ? "" : Number(v).toFixed(2);
          } else if (col.key === "sessions_per_sec") {
            applyMonoTableCellClass(td, col.key);
            td.textContent = formatAshSessionsPerSec(v);
          } else if (col.key === "rows_per_call") {
            applyMonoTableCellClass(td, col.key);
            const raw =
              row.rows_per_call != null && row.rows_per_call !== ""
                ? row.rows_per_call
                : row.avg_rows_per_call;
            td.textContent = raw === null || raw === undefined ? "" : String(raw);
          } else {
            applyMonoTableCellClass(td, col.key);
            td.textContent = v === null || v === undefined ? "" : String(v);
          }
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    }

    const firstTh = trh.querySelector(`th[data-sort-key="${state.key}"]`);
    if (firstTh) firstTh.classList.add(state.dir === "asc" ? "sort-asc" : "sort-desc");
    renderBody();
    body.appendChild(table);
    return section;
  }

  function buildSortablePaginatedTable(
    titleBase,
    rows,
    columns,
    pageSize,
    subsectionId,
    initialSort,
    tableOptions
  ) {
    const opt = tableOptions || {};
    const unifyStatementHeaders = !!opt.unifyStatementHeaders;
    const pgssAshLinks = !!opt.pgssAshLinks;
    const section = el("section", { className: "ybtop-section" });
    const h2 = el("h2", { className: "section-title" });
    const body = el("div", { className: "section-body" });
    if (subsectionId) {
      const header = el("div", { className: "section-header" });
      const toggle = el("button", { type: "button", className: "section-toggle" });
      header.appendChild(toggle);
      header.appendChild(h2);
      section.appendChild(header);
      wireSubsectionCollapse(section, subsectionId, body, toggle);
    } else {
      section.appendChild(h2);
    }
    section.appendChild(body);

    const pager = el("div", { className: "pager" });
    if (!rows.length) {
      h2.textContent = titleBase;
      body.appendChild(el("p", { textContent: "(no rows)" }));
      return section;
    }

    const state = {
      key: (initialSort && initialSort.key) || columns[0].key,
      dir: (initialSort && initialSort.dir) || "desc",
      page: 1,
    };

    function totalPages() {
      return Math.max(1, Math.ceil(rows.length / pageSize));
    }

    function cmp(a, b) {
      const col = columns.find((c) => c.key === state.key) || columns[0];
      let va;
      let vb;
      if (typeof col.sortValue === "function") {
        va = col.sortValue(a);
        vb = col.sortValue(b);
      } else {
        va = a[state.key];
        vb = b[state.key];
      }
      if (col.type === "number") {
        va = Number(va) || 0;
        vb = Number(vb) || 0;
      } else {
        va = String(va || "").toLowerCase();
        vb = String(vb || "").toLowerCase();
      }
      if (va < vb) return state.dir === "asc" ? -1 : 1;
      if (va > vb) return state.dir === "asc" ? 1 : -1;
      return 0;
    }

    function sortedRows() {
      return rows.slice().sort(cmp);
    }

    function updateHeading() {
      const tp = totalPages();
      h2.textContent = `${titleBase} — page ${state.page} of ${tp} (${rows.length} rows)`;
    }

    const table = el("table");
    const thead = el("thead");
    const trh = el("tr");
    columns.forEach((col) => {
      const th = el("th");
      appendColumnHeader(th, col, { unifyStatementHeaders });
      th.addEventListener("click", () => {
        if (state.key === col.key) state.dir = state.dir === "asc" ? "desc" : "asc";
        else {
          state.key = col.key;
          state.dir = col.type === "number" ? "desc" : "asc";
        }
        state.page = 1;
        trh.querySelectorAll("th").forEach((x) => {
          x.classList.remove("sort-asc", "sort-desc");
        });
        th.classList.add(state.dir === "asc" ? "sort-asc" : "sort-desc");
        renderAll();
      });
      trh.appendChild(th);
    });
    thead.appendChild(trh);
    table.appendChild(thead);
    const tbody = el("tbody");
    table.appendChild(tbody);

    function renderBody() {
      tbody.textContent = "";
      const sorted = sortedRows();
      const tp = totalPages();
      const p = Math.min(Math.max(1, state.page), tp);
      state.page = p;
      const start = (p - 1) * pageSize;
      const slice = sorted.slice(start, start + pageSize);
      slice.forEach((row) => {
        const tr = el("tr");
        columns.forEach((col) => {
          const td = el("td");
          const v = row[col.key];
          if (col.key === "query") {
            applyMonoTableCellClass(td, col.key);
            if (pgssAshLinks && row.queryid != null && String(row.queryid) !== "") {
              appendQueryCellWithAshLinks(td, v, row.queryid);
            } else {
              appendQueryCell(td, v);
            }
          } else if (col.key === "queryid" && pgssAshLinks && row.queryid != null && String(row.queryid) !== "") {
            applyMonoTableCellClass(td, col.key);
            const a = el("a", {
              className: "ash-queryid-deeplink",
              href: buildAshQueryHref(row.queryid),
              textContent: v === null || v === undefined ? "" : String(v),
            });
            a.addEventListener("click", (e) => {
              e.preventDefault();
              navigateToAshForQueryId(row.queryid);
            });
            td.appendChild(a);
          } else if (col.key === "per_node_counts") {
            appendTabletCountStripCell(td, v);
          } else if (col.key === "load_pct" || col.key === "time_pct") {
            applyMonoTableCellClass(td, col.key);
            td.textContent =
              v === null || v === undefined || v === "" ? "" : `${Number(v).toFixed(2)}%`;
          } else if (col.key === "calls_per_sec") {
            applyMonoTableCellClass(td, col.key);
            td.textContent =
              v === null || v === undefined || v === "" ? "" : Number(v).toFixed(2);
          } else if (col.key === "sessions_per_sec") {
            applyMonoTableCellClass(td, col.key);
            td.textContent = formatAshSessionsPerSec(v);
          } else if (col.key === "rows_per_call") {
            applyMonoTableCellClass(td, col.key);
            const raw =
              row.rows_per_call != null && row.rows_per_call !== ""
                ? row.rows_per_call
                : row.avg_rows_per_call;
            td.textContent = raw === null || raw === undefined ? "" : String(raw);
          } else {
            applyMonoTableCellClass(td, col.key);
            td.textContent = v === null || v === undefined ? "" : String(v);
          }
          tr.appendChild(td);
        });
        tbody.appendChild(tr);
      });
    }

    function renderPager() {
      pager.textContent = "";
      const tp = totalPages();

      const prev = el("button", { type: "button", className: "pager-btn", textContent: "‹ Prev" });
      prev.disabled = state.page <= 1;
      prev.addEventListener("click", () => {
        if (state.page > 1) {
          state.page -= 1;
          renderAll();
        }
      });
      pager.appendChild(prev);

      if (tp <= 1) {
        const b = el("button", {
          type: "button",
          className: "pager-btn pager-btn-current",
          textContent: "1",
          "aria-label": "Page 1 of 1",
        });
        b.disabled = true;
        pager.appendChild(b);
      } else {
        const pages = new Set([1, tp, state.page]);
        for (let d = -3; d <= 3; d += 1) {
          const x = state.page + d;
          if (x >= 1 && x <= tp) pages.add(x);
        }
        const sortedPages = Array.from(pages).sort((a, b) => a - b);
        let last = 0;
        sortedPages.forEach((pnum) => {
          if (last && pnum > last + 1) {
            pager.appendChild(el("span", { className: "pager-ellipsis", textContent: "…" }));
          }
          const b = el("button", {
            type: "button",
            className: "pager-btn" + (pnum === state.page ? " pager-btn-current" : ""),
            textContent: String(pnum),
          });
          b.addEventListener("click", () => {
            state.page = pnum;
            renderAll();
          });
          pager.appendChild(b);
          last = pnum;
        });
      }

      const next = el("button", { type: "button", className: "pager-btn", textContent: "Next ›" });
      next.disabled = state.page >= tp;
      next.addEventListener("click", () => {
        if (state.page < tp) {
          state.page += 1;
          renderAll();
        }
      });
      pager.appendChild(next);
    }

    function renderAll() {
      updateHeading();
      renderBody();
      renderPager();
    }

    body.appendChild(table);
    body.appendChild(pager);

    const firstTh = trh.querySelector(`th[data-sort-key="${state.key}"]`);
    if (firstTh) firstTh.classList.add(state.dir === "asc" ? "sort-asc" : "sort-desc");
    renderAll();
    return section;
  }

  function renderDoc(doc, prevDoc) {
    const app = document.getElementById("app");
    const nav = document.getElementById("app-nav");
    app.textContent = "";
    if (nav) nav.textContent = "";
    lastDoc = doc;
    lastPrevDoc = prevDoc;

    const st = doc.pg_stat_statements && doc.pg_stat_statements.per_node;
    const ash = doc.yb_active_session_history && doc.yb_active_session_history.per_node;
    const topo = doc.node_topology || {};

    const panelPgss = el("div", {
      className: "app-panel",
      "data-viewer-section": "pgss",
      role: "tabpanel",
      id: "panel-pgss",
      "aria-labelledby": "tab-pgss",
    });
    const panelAsh = el("div", {
      className: "app-panel",
      "data-viewer-section": "ash",
      role: "tabpanel",
      id: "panel-ash",
      "aria-labelledby": "tab-ash",
    });
    const panelTablets = el("div", {
      className: "app-panel",
      "data-viewer-section": "tablets",
      role: "tabpanel",
      id: "panel-tablets",
      "aria-labelledby": "tab-tablets",
    });

    if (st) {
      const merged = mergeStatements(st);
      const prevSt = prevDoc && prevDoc.pg_stat_statements && prevDoc.pg_stat_statements.per_node;
      let pgTitle = "Top 25 — pg_stat_statements";
      let pgRows = withPgStatTimePercent(merged);
      let pgCols = pgStatStatementColumns(pgRows, st);
      const pgSort = { key: "total_ms", dir: "desc" };
      if (prevDoc && prevSt) {
        const mergedPrev = mergeStatements(prevSt);
        const deltaRows = deltaPgStatMergedRows(merged, mergedPrev);
        panelPgss.appendChild(
          pgStatActivityBannerDelta(prevDoc.generated_at_utc, doc.generated_at_utc)
        );
        pgTitle = "Top 25 — pg_stat_statements (Δ vs prior snapshot)";
        pgRows = withPgStatDeltaDerivedRows(
          deltaRows,
          prevDoc.generated_at_utc,
          doc.generated_at_utc
        );
        pgCols = pgStatStatementColumnsDelta(pgRows, st);
      } else {
        panelPgss.appendChild(pgStatActivityBannerAt(doc.generated_at_utc));
        if (prevDoc && !prevSt) {
          panelPgss.appendChild(
            el("div", {
              className: "pgss-activity-note",
              textContent:
                "Previous snapshot has no pg_stat_statements data; showing cumulative totals for this snapshot.",
            })
          );
        }
      }
      panelPgss.appendChild(
        buildSortablePaginatedTable(pgTitle, pgRows, pgCols, 25, "sec-pgss-main", pgSort, {
          unifyStatementHeaders: true,
          pgssAshLinks: true,
        })
      );
    } else {
      panelPgss.appendChild(
        el("p", {
          className: "app-panel-empty",
          textContent: "No pg_stat_statements.per_node in this snapshot.",
        })
      );
    }

    if (ash) {
      panelAsh.appendChild(ashWindowActivityBanner(doc));
      const qF = ashQueryIdFilter;
      const ashData = qF ? filterAshPerNodeByQueryId(ash, qF) : ash;
      if (qF) {
        const qText = getQueryTextForToolbar(doc, qF) || "";
        const note = el("div", { className: "ash-mode-banner" });
        note.appendChild(
          el("div", {
            className: "ash-mode-banner-title",
            textContent: `Showing ASH Data for query_id=${qF}:`,
          })
        );
        const pre = el("pre", { className: "ash-mode-banner-body" });
        pre.appendChild(el("span", { className: "ash-mode-banner-sql", textContent: ` query=${qText}` }));
        note.appendChild(pre);
        panelAsh.appendChild(note);
      }
      const mergedAsh = mergeAsh(ashData);
      const ashIntervalSec = ashWindowIntervalSeconds(doc);
      const ashEnriched = (rows) => withAshLoadPercent(withAshSessionsPerSec(rows, ashIntervalSec));
      const ASH_SPS_COL = {
        key: "sessions_per_sec",
        type: "number",
        label: "Active Sessions / sec",
      };
      const ASH_LOAD_COL = { key: "load_pct", label: "Load %", type: "number" };
      const mergedAshL = ashEnriched(mergedAsh);
      const ashMainColsAll = [
        ASH_SPS_COL,
        ASH_LOAD_COL,
        { key: "namespace_name", label: "namespace" },
        { key: "object_name", label: "object_name" },
        { key: "wait_event_component", label: "wait_event_component" },
        { key: "wait_event_type", label: "wait_event_type" },
        { key: "wait_event", label: "wait_event" },
        { key: "query_id", label: "query_id" },
        { key: "query", label: "query" },
      ];
      const ashMainCols = qF ? ashColumnsWithoutQueryIdAndQuery(ashMainColsAll) : ashMainColsAll;
      panelAsh.appendChild(
        buildSortablePaginatedTable(
          "Top 50 — ASH by samples",
          mergedAshL,
          ashMainCols,
          50,
          "sec-ash-main"
        )
      );

      if (!qF) {
        const byNsQuery = ashEnriched(groupAshByNamespaceQuery(mergedAsh));
        const byNsQueryColsAll = [
          ASH_SPS_COL,
          ASH_LOAD_COL,
          { key: "namespace_name", label: "namespace" },
          { key: "query", label: "query" },
        ];
        const byNsQueryCols = byNsQueryColsAll;
        const byNsQueryTitle = `ASH by namespace + query (${byNsQuery.length} groups)`;
        panelAsh.appendChild(
          buildSortableTable(byNsQueryTitle, byNsQuery, byNsQueryCols, "sec-ash-ns-q")
        );

        const byNsObj = groupSum(mergedAsh, (r) => {
          const nn = r.namespace_name || "";
          const on = r.object_name || "";
          return `${nn}\0${on}`;
        }).map((x) => ({
          namespace_name: String(x.key).split("\0")[0],
          object_name: String(x.key).split("\0")[1],
          samples: x.samples,
        }));
        const byNsObjTop = withAshLoadPercent(
          withAshSessionsPerSec(byNsObj.slice(0, 50), ashIntervalSec),
          byNsObj
        );
        panelAsh.appendChild(
          buildSortableTable(
            "Top 50 — ASH by namespace + object_name",
            byNsObjTop,
            [
              ASH_SPS_COL,
              ASH_LOAD_COL,
              { key: "namespace_name", label: "namespace" },
              { key: "object_name", label: "object_name" },
            ],
            "sec-ash-ns-obj"
          )
        );
      }

      const byNsObjQuery = ashEnriched(groupAshByNamespaceObjectQuery(mergedAsh));
      const byNsObjQueryTitle = qF
        ? `ASH by namespace + object_name (${byNsObjQuery.length} groups)`
        : `ASH by namespace + object_name + query (${byNsObjQuery.length} groups)`;
      const byNsObjQueryColsAll = [
        ASH_SPS_COL,
        ASH_LOAD_COL,
        { key: "namespace_name", label: "namespace" },
        { key: "object_name", label: "object_name" },
        { key: "query", label: "query" },
      ];
      const byNsObjQueryCols = qF ? ashColumnsWithoutQueryIdAndQuery(byNsObjQueryColsAll) : byNsObjQueryColsAll;
      panelAsh.appendChild(
        buildSortableTable(byNsObjQueryTitle, byNsObjQuery, byNsObjQueryCols, "sec-ash-ns-obj-q")
      );

      const byDb = ashEnriched(
        groupSum(mergedAsh, (r) => String(r.namespace_name || "(none)")).map((x) => ({
          namespace_name: x.key,
          samples: x.samples,
        }))
      );
      panelAsh.appendChild(
        buildSortableTable(
          "ASH samples by database",
          byDb,
          [ASH_SPS_COL, ASH_LOAD_COL, { key: "namespace_name", label: "namespace" }],
          "sec-ash-db"
        )
      );

      const flat = flattenAsh(ashData, topo);
      const byNode = ashEnriched(sumAshByNode(flat));
      panelAsh.appendChild(
        buildSortableTable(
          "ASH samples by node",
          byNode,
          [
            ASH_SPS_COL,
            ASH_LOAD_COL,
            { key: "node_id", label: "node_id" },
            { key: "cloud", label: "cloud" },
            { key: "region", label: "region" },
            { key: "zone", label: "zone" },
          ],
          "sec-ash-node"
        )
      );

      const byCrz = ashEnriched(
        groupSum(flat, (r) => `${r.cloud}\t${r.region}\t${r.zone}`).map((x) => {
          const parts = String(x.key).split("\t");
          return {
            cloud: parts[0] != null ? parts[0] : "",
            region: parts[1] != null ? parts[1] : "",
            zone: parts[2] != null ? parts[2] : "",
            samples: x.samples,
          };
        })
      );
      panelAsh.appendChild(
        buildSortableTable(
          "ASH by cloud + region + zone",
          byCrz,
          [
            ASH_SPS_COL,
            ASH_LOAD_COL,
            { key: "cloud", label: "cloud" },
            { key: "region", label: "region" },
            { key: "zone", label: "zone" },
          ],
          "sec-ash-crz"
        )
      );
    } else {
      panelAsh.appendChild(
        el("p", {
          className: "app-panel-empty",
          textContent: "No yb_active_session_history.per_node in this snapshot.",
        })
      );
    }

    const lt = doc.yb_local_tablets && doc.yb_local_tablets.per_node;
    const hasLocalTablets =
      lt &&
      Object.keys(lt).some((nid) => Array.isArray(lt[nid]) && lt[nid].length > 0);
    if (hasLocalTablets) {
      const perTable = tabletsPerTableReport(lt);
      panelTablets.appendChild(
        buildSortableTable(
          "Tablet Distribution - By Table",
          perTable,
          [
            { key: "tablets", label: "tablets", type: "number" },
            { key: "namespace_name", label: "namespace" },
            { key: "table_name", label: "table_name" },
            {
              key: "per_node_counts",
              label: "per-node counts",
              type: "number",
              sortValue: (r) => {
                const a = r.per_node_counts;
                if (!a || !a.length) return 0;
                return Math.max(...a.map((x) => x.count));
              },
            },
          ],
          "sec-lt-per-table"
        )
      );
      panelTablets.appendChild(
        buildSortableTable(
          "Tablet Distribution - By Node",
          tabletsPerNodeReport(lt, topo),
          [
            { key: "tablets", label: "tablets", type: "number" },
            { key: "node_id", label: "node_id" },
            { key: "cloud", label: "cloud" },
            { key: "region", label: "region" },
            { key: "zone", label: "zone" },
          ],
          "sec-lt-per-node"
        )
      );
      panelTablets.appendChild(
        buildSortableTable(
          "Tablet Distribution - By Cloud:Region:Zone",
          tabletsPerCloudRegionZoneReport(lt, topo),
          [
            { key: "tablets", label: "tablets", type: "number" },
            { key: "cloud", label: "cloud" },
            { key: "region", label: "region" },
            { key: "zone", label: "zone" },
          ],
          "sec-lt-per-crz"
        )
      );
    } else {
      panelTablets.appendChild(
        el("p", {
          className: "app-panel-empty",
          textContent: "No yb_local_tablets.per_node data in this snapshot.",
        })
      );
    }

    app.appendChild(panelPgss);
    app.appendChild(panelAsh);
    app.appendChild(panelTablets);
    buildViewerNav();
    updateAshFilterToolbar();
  }

  async function showSnapshotAt(index) {
    const app = document.getElementById("app");
    if (!manifestEntries.length) {
      const nav0 = document.getElementById("app-nav");
      if (nav0) nav0.textContent = "";
      app.textContent = "No entries in ybtop.manifest.json";
      return;
    }
    if (index < 0 || index >= manifestEntries.length) return;
    currentIndex = index;
    const ent = manifestEntries[index];
    const label = document.getElementById("nav-label");
    label.textContent = `${index + 1} / ${manifestEntries.length} — ${ent.file || ""}`;

    document.getElementById("btn-prev").disabled = index <= 0;
    document.getElementById("btn-next").disabled = index >= manifestEntries.length - 1;
    document.getElementById("btn-first").disabled = index <= 0;
    document.getElementById("btn-last").disabled = index >= manifestEntries.length - 1;

    app.textContent = "Loading…";
    const navEl = document.getElementById("app-nav");
    if (navEl) navEl.textContent = "";
    setStatus("", false);
    const name = ent.file;
    try {
      const prevName = index > 0 ? manifestEntries[index - 1].file : null;
      const [doc, prevDoc] = await Promise.all([
        fetchJson(name),
        prevName ? fetchJson(prevName).catch(() => null) : Promise.resolve(null),
      ]);
      app.textContent = "";
      renderDoc(doc, prevDoc);
      setStatus("OK", false);
    } catch (e) {
      const navErr = document.getElementById("app-nav");
      if (navErr) navErr.textContent = "";
      app.textContent = "";
      const banner = el("div", { className: "err-banner" });
      banner.textContent = `Could not load ${name}: ${e.message || e}. Use First, Last, Prev, or Next to try another snapshot.`;
      app.appendChild(banner);
      setStatus("Load failed", true);
    }
  }

  function wireNav() {
    document.getElementById("btn-prev").addEventListener("click", () => {
      if (currentIndex > 0) showSnapshotAt(currentIndex - 1);
    });
    document.getElementById("btn-next").addEventListener("click", () => {
      if (currentIndex < manifestEntries.length - 1) showSnapshotAt(currentIndex + 1);
    });
    document.getElementById("btn-first").addEventListener("click", () => {
      if (currentIndex > 0) showSnapshotAt(0);
    });
    document.getElementById("btn-last").addEventListener("click", () => {
      if (manifestEntries.length > 0 && currentIndex < manifestEntries.length - 1) {
        showSnapshotAt(manifestEntries.length - 1);
      }
    });
  }

  function clearYbtopVersionPlaceholder() {
    const vEl = document.getElementById("ybtop-version");
    if (vEl && vEl.textContent.indexOf("__YBTOP_VERSION__") !== -1) {
      vEl.textContent = "";
    }
  }

  async function boot() {
    clearYbtopVersionPlaceholder();
    wireNav();
    window.addEventListener("popstate", () => {
      readViewerStateFromUrl();
      if (lastDoc) {
        renderDoc(lastDoc, lastPrevDoc);
      } else {
        updateAshFilterToolbar();
      }
    });
    try {
      manifestEntries = await loadManifest();
    } catch (e) {
      const navM = document.getElementById("app-nav");
      if (navM) navM.textContent = "";
      document.getElementById("app").textContent = `Failed to load ${MANIFEST}: ${e.message || e}`;
      setStatus("Manifest error", true);
      return;
    }
    if (!manifestEntries.length) {
      const navE = document.getElementById("app-nav");
      if (navE) navE.textContent = "";
      document.getElementById("app").textContent = "Manifest has no entries.";
      return;
    }
    readViewerStateFromUrl();
    showSnapshotAt(manifestEntries.length - 1);
  }

  boot();
})();
