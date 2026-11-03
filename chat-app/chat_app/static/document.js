/* IDX Research Desk — the analysis vault.
 *
 * Two routes:
 *   #/documents      the library (filters over GET /api/documents)
 *   #/doc/{id}       the reader + document-scoped chat (GET /api/documents/{id})
 *
 * The reader is the full markdown file. The renderer is in shared.js and is
 * escape-first; this file only composes chrome, outline, scroll-spy, selection
 * capture and the pinned chat.
 */

"use strict";

(function (global) {
  const RD = global.RD;
  if (!RD) return;

  const MAX_QUESTION = 2000;

  /* ========================================================================
     Library — #/documents
     ======================================================================== */

  const libraryState = {
    q: "",
    ticker: "",
    period: "",
    kind: "",
    sort: "recent",
    instance: null,
  };

  function libShell() {
    return '<div class="library">' +
      '<header class="library__head">' +
        '<p class="eyebrow"><span class="eyebrow__no">01</span>The vault</p>' +
        '<div class="library__title-row">' +
          '<h1 class="library__title">Analysis notes</h1>' +
          '<p class="library__edition">Read everything.<br>Trust the ledger.</p>' +
        '</div>' +
        '<p class="library__standfirst">The full research vault — company notes, sector pieces, commodity work and articles. Reading is not gated by the fact-sheet supported set. Every note opens whole, with its tables, and every note chat cites the section it used.</p>' +
        '<div class="library__meta">' +
          '<span class="status-dot" data-state="loading" data-lib-dot aria-hidden="true"></span>' +
          '<span id="library-count" aria-live="polite">Reading the index…</span>' +
          '<span class="library__meta-sep" aria-hidden="true">·</span>' +
          '<span id="library-as-of">as of —</span>' +
          '<span class="library__meta-sep" aria-hidden="true">·</span>' +
          '<span class="library__meta-note">secondary notes, never primary filings</span>' +
        '</div>' +
      '</header>' +
      '<section class="library__toolbar" aria-label="Filter the vault">' +
        '<label class="filter filter--search">' +
          '<span class="filter__label">Search</span>' +
          '<input id="library-q" type="search" autocomplete="off" spellcheck="false" placeholder="Title, ticker, tag or alias…">' +
        '</label>' +
        '<label class="filter"><span class="filter__label">Issuer</span>' +
          '<span class="select-wrap"><select id="library-ticker"><option value="">All issuers</option></select></span></label>' +
        '<label class="filter"><span class="filter__label">Period</span>' +
          '<span class="select-wrap"><select id="library-period"><option value="">All periods</option></select></span></label>' +
        '<label class="filter"><span class="filter__label">Kind</span>' +
          '<span class="select-wrap"><select id="library-kind"><option value="">All kinds</option></select></span></label>' +
        '<label class="filter"><span class="filter__label">Order</span>' +
          '<span class="select-wrap"><select id="library-sort">' +
            '<option value="recent">Recently updated</option>' +
            '<option value="title">Title A–Z</option>' +
            '<option value="issuer">Issuer, then period</option>' +
          '</select></span></label>' +
        '<button type="button" class="text-button library__clear" id="library-clear" hidden>Clear filters</button>' +
      '</section>' +
      '<div id="library-body">' + libLoading() + "</div>" +
      '<footer class="desk-foot">' +
        '<p>IDX Research Desk · analysis vault</p>' +
        '<p class="desk-foot__aside">Notes are secondary evidence. Verify a number against the filings.</p>' +
      '</footer>' +
    "</div>";
  }

  function libLoading() {
    return '<div class="skeleton-grid" aria-hidden="true">' +
      Array.from({ length: 6 }).map(() =>
        '<div class="skeleton-card"><span></span><span></span><span></span></div>'
      ).join("") +
    "</div><p class=\"sr-only\">Reading the document index…</p>";
  }

  function libErrorState(error) {
    const message = error && error.message ? error.message : "The document index is unreachable.";
    const notReady = /404|not found/i.test(message);
    return '<section class="library__state" aria-labelledby="library-error-title">' +
      '<span class="panel__stamp">' + (notReady ? "Not wired" : "Offline") + "</span>" +
      '<h2 class="panel__title" id="library-error-title">' +
        (notReady ? "The document API is not available yet." : "The vault index did not answer.") +
      "</h2>" +
      '<p class="panel__body">' +
        (notReady
          ? "The static shell is running, but GET /api/documents is not served. The library will fill in as soon as the document API lands; the desk remains usable."
          : "The browser could not read the local document index. Check that the app is running on 127.0.0.1:8765, then retry.") +
      "</p>" +
      '<p class="panel__error">' + RD.esc(message) + "</p>" +
      '<div class="panel__actions">' +
        '<button type="button" class="button-quiet" data-library-retry>Retry index</button>' +
        '<a class="button-quiet" href="#/desk">Open the desk →</a>' +
      "</div>" +
    "</section>";
  }

  function libEmptyState(filtered, docs) {
    const hasFilters = Boolean(libraryState.q || libraryState.ticker || libraryState.period || libraryState.kind);
    const scope = [libraryState.ticker, libraryState.period, libraryState.kind].filter(Boolean).join(" · ");
    let kicker = "Nothing indexed";
    let title = "The vault is empty.";
    let body = "No analysis notes were returned by the index. This is an index state, not a claim that the notes do not exist.";
    let actions = '<button type="button" class="button-quiet" data-library-retry>Retry index</button>';

    if (docs.length && hasFilters) {
      kicker = "No note on file";
      title = scope ? "No note for " + scope + "." : "No notes match this filter.";
      body = scope
        ? "The vault has no note for this scope. A supported issuer and period can exist without an analysis note; that is different from a missing filing. Clear the filters to read the rest of the vault, or ask the desk to check primary sources."
        : "No note matches the current filters. Clear them to read the rest of the vault.";
      const periodMatch = /^(Q[1-4])\s+(\d{4})$/i.exec(String(libraryState.period || ""));
      actions = '<button type="button" class="button-quiet" data-library-clear>Clear filters</button>' +
        '<a class="button-quiet" href="' + RD.escapeAttr(RD.deskHref({
          ticker: libraryState.ticker || "",
          period: periodMatch ? periodMatch[1] : "",
          year: periodMatch ? Number(periodMatch[2]) : "",
          q: scope ? "What primary documents and fact sheets exist for " + scope + "?" : "Which analysis notes exist for this scope?",
        })) + '">Ask the desk about this scope →</a>';
    }

    if (!filtered && !docs.length) {
      kicker = "Index empty";
      title = "The vault returned no notes.";
    }

    return '<section class="library__state library__state--empty" aria-labelledby="library-empty-title">' +
      '<p class="empty__kicker">' + RD.esc(kicker) + "</p>" +
      '<h2 class="panel__title" id="library-empty-title">' + RD.esc(title) + "</h2>" +
      '<p class="panel__body">' + RD.esc(body) + "</p>" +
      '<div class="panel__actions">' + actions + "</div>" +
    "</section>";
  }

  function uniqueSorted(values) {
    return Array.from(new Set(values.filter(Boolean).map(String))).sort((a, b) => a.localeCompare(b));
  }

  function periodSortValue(period) {
    const match = /^Q([1-4])\s+(\d{4})$/i.exec(String(period || "").trim());
    if (!match) return 0;
    return Number(match[2]) * 10 + Number(match[1]);
  }

  function docPeriodLabel(doc) {
    if (!doc || !doc.period) return "";
    return [doc.period, doc.year].filter(Boolean).join(" ");
  }

  function optionsHtml(values, selected, allLabel) {
    return '<option value="">' + RD.esc(allLabel) + "</option>" +
      values.map((value) =>
        '<option value="' + RD.escapeAttr(value) + '"' +
        (String(value) === String(selected) ? " selected" : "") + ">" + RD.esc(value) + "</option>"
      ).join("");
  }

  function syncLibraryFilters(inst) {
    const docs = RD.documents.all();
    const tickers = uniqueSorted(docs.map((doc) => doc.ticker));
    const periods = uniqueSorted(docs.map(docPeriodLabel)).sort((a, b) => periodSortValue(b) - periodSortValue(a));
    const kinds = uniqueSorted(docs.map((doc) => doc.kind));

    if (!tickers.includes(libraryState.ticker)) libraryState.ticker = "";
    if (!periods.includes(libraryState.period)) libraryState.period = "";
    if (!kinds.includes(libraryState.kind)) libraryState.kind = "";

    inst.ticker.innerHTML = optionsHtml(tickers, libraryState.ticker, "All issuers");
    inst.period.innerHTML = optionsHtml(periods, libraryState.period, "All periods");
    inst.kind.innerHTML = optionsHtml(kinds, libraryState.kind, "All kinds");
    inst.ticker.value = libraryState.ticker;
    inst.period.value = libraryState.period;
    inst.kind.value = libraryState.kind;
    inst.sort.value = libraryState.sort;
  }

  function matchesQuery(doc, query) {
    if (!query) return true;
    const haystack = [
      doc.title,
      doc.ticker,
      doc.period,
      doc.year,
      doc.kind,
      doc.path,
      asText(doc.tags),
      asText(doc.aliases),
    ].join(" ").toLowerCase();
    return query.split(/\s+/).filter(Boolean).every((token) => haystack.includes(token));
  }

  function asText(value) {
    return RD.asArray(value).join(" ");
  }

  function filteredDocs() {
    const query = libraryState.q.trim().toLowerCase();
    const docs = RD.documents.all().filter((doc) => {
      if (libraryState.ticker && doc.ticker !== libraryState.ticker) return false;
      if (libraryState.period && docPeriodLabel(doc) !== libraryState.period) return false;
      if (libraryState.kind && doc.kind !== libraryState.kind) return false;
      return matchesQuery(doc, query);
    });

    return docs.sort((a, b) => {
      if (libraryState.sort === "title") return String(a.title).localeCompare(String(b.title));
      if (libraryState.sort === "issuer") {
        const left = (a.ticker || a.kind || "") + " " + (periodSortValue(a.period) * -1);
        const right = (b.ticker || b.kind || "") + " " + (periodSortValue(b.period) * -1);
        return left.localeCompare(right) || String(a.title).localeCompare(String(b.title));
      }
      const left = Date.parse(a.updated_at || "") || 0;
      const right = Date.parse(b.updated_at || "") || 0;
      return right - left || String(a.title).localeCompare(String(b.title));
    });
  }

  function docCard(doc) {
    const sections = doc.sections.length || Number(doc.section_count) || 0;
    const coverage = doc.coverage ? RD.coverage.label(doc.coverage) : "";
    const stale = isStale(doc);
    const chips = [];
    if (doc.ticker) chips.push('<span class="chip chip--ticker">' + RD.esc(doc.ticker) + "</span>");
    if (doc.period || doc.year) {
      chips.push('<span class="chip">' + RD.esc([doc.period, doc.year].filter(Boolean).join(" ")) + "</span>");
    }
    if (!doc.ticker) chips.push('<span class="chip chip--kind">' + RD.esc(RD.humanState(doc.kind)) + "</span>");
    if (stale) chips.push('<span class="chip chip--warn">stale</span>');

    return '<article class="doc-card">' +
      '<div class="doc-card__top">' +
        '<span class="doc-card__kind">' + RD.esc(RD.humanState(doc.kind)) + "</span>" +
        '<span class="doc-card__updated">Updated ' + RD.esc(RD.formatDate(doc.updated_at)) + "</span>" +
      "</div>" +
      '<h2 class="doc-card__title"><a href="' + RD.escapeAttr(RD.docHref(doc.id)) + '">' +
        RD.esc(doc.title) + "</a></h2>" +
      '<div class="doc-card__chips">' + chips.join("") + "</div>" +
      '<dl class="doc-card__stats">' +
        "<div><dt>Sections</dt><dd>" + (sections || "—") + "</dd></div>" +
        "<div><dt>Words</dt><dd>" + (doc.words ? RD.formatNumber(Number(doc.words)) : "—") + "</dd></div>" +
        "<div><dt>Size</dt><dd>" + RD.esc(RD.formatBytes(doc.bytes)) + "</dd></div>" +
      "</dl>" +
      '<p class="doc-card__coverage"><span>Coverage</span> ' +
        (coverage ? RD.esc(coverage) : '<em>not recorded</em>') + "</p>" +
      '<div class="doc-card__foot">' +
        '<span class="doc-card__id">' + RD.esc(doc.id) + "</span>" +
        '<a class="doc-card__open" href="' + RD.escapeAttr(RD.docHref(doc.id)) + '">Open note <span aria-hidden="true">→</span></a>' +
      "</div>" +
    "</article>";
  }

  function isStale(doc) {
    if (!doc) return false;
    if (doc.stale === true) return true;
    const completeness = RD.objectOr(doc.completeness);
    return completeness.analysis === "stale" || completeness.notes === "stale" || doc.freshness === "stale";
  }

  function renderLibraryList(inst) {
    if (!inst.alive) return;
    const docs = RD.documents.all();
    const filtered = filteredDocs();
    const store = RD.documents.state();

    inst.count.textContent = docs.length
      ? filtered.length + " of " + docs.length + " note" + (docs.length === 1 ? "" : "s")
      : "No notes indexed";
    inst.asOf.textContent = "as of " + RD.formatDate(store.asOf || RD.state.asOf);
    const dot = inst.count.parentElement.querySelector("[data-lib-dot]");
    if (dot) dot.dataset.state = "ready";

    if (inst.clear) {
      inst.clear.hidden = !(libraryState.q || libraryState.ticker || libraryState.period || libraryState.kind);
    }

    if (!filtered.length) {
      inst.body.innerHTML = libEmptyState(filtered, docs);
      return;
    }

    inst.body.innerHTML = '<div class="library__result-head">' +
      '<p>' + filtered.length + " note" + (filtered.length === 1 ? "" : "s") +
        (libraryState.q ? ' for “' + RD.esc(libraryState.q) + '”' : "") + "</p>" +
      '<p class="library__result-scope">' +
        RD.esc([libraryState.ticker, libraryState.period, libraryState.kind].filter(Boolean).join(" · ") || "all scopes") +
      "</p></div>" +
      '<div class="doc-grid">' + filtered.map(docCard).join("") + "</div>";
  }

  function bindLibrary(inst) {
    const root = inst.root;

    function onReady() {
      if (!inst.alive) return;
      try {
        syncLibraryFilters(inst);
        renderLibraryList(inst);
      } catch (error) {
        inst.body.innerHTML = libErrorState(error);
      }
    }

    inst.onDocuments = onReady;
    window.addEventListener("rd:documents", inst.onDocuments);

    inst.q.addEventListener("input", RD.debounce(() => {
      libraryState.q = inst.q.value;
      renderLibraryList(inst);
    }, 120));

    [["ticker", inst.ticker], ["period", inst.period], ["kind", inst.kind]].forEach(([key, select]) => {
      select.addEventListener("change", () => {
        libraryState[key] = select.value;
        renderLibraryList(inst);
      });
    });

    inst.sort.addEventListener("change", () => {
      libraryState.sort = inst.sort.value;
      renderLibraryList(inst);
    });

    inst.clear.addEventListener("click", () => {
      libraryState.q = "";
      libraryState.ticker = "";
      libraryState.period = "";
      libraryState.kind = "";
      inst.q.value = "";
      inst.ticker.value = "";
      inst.period.value = "";
      inst.kind.value = "";
      renderLibraryList(inst);
      inst.q.focus();
      RD.announce("Filters cleared. Showing the whole vault.");
    });

    root.addEventListener("click", (event) => {
      if (event.target.closest("[data-library-retry]")) {
        loadLibrary(inst, true);
        return;
      }
      if (event.target.closest("[data-library-clear]")) {
        inst.clear.click();
      }
    });

    function loadLibrary(instance, force) {
      const store = RD.documents.state();
      if (store.loaded && !force) {
        onReady();
        return;
      }
      if (!store.loading) instance.body.innerHTML = libLoading();
      RD.documents.load(force).then(() => {
        if (!instance.alive) return;
        onReady();
      }).catch((error) => {
        if (!instance.alive) return;
        const dot = document.querySelector("[data-lib-dot]");
        if (dot) dot.dataset.state = "error";
        instance.count.textContent = "Index unavailable";
        instance.asOf.textContent = "as of —";
        instance.body.innerHTML = libErrorState(error);
      });
    }
    inst.load = loadLibrary;
    loadLibrary(inst, false);
  }

  const libraryView = {
    mount(root) {
      root.innerHTML = libShell();
      const inst = {
        root: root,
        alive: true,
        body: root.querySelector("#library-body"),
        q: root.querySelector("#library-q"),
        ticker: root.querySelector("#library-ticker"),
        period: root.querySelector("#library-period"),
        kind: root.querySelector("#library-kind"),
        sort: root.querySelector("#library-sort"),
        clear: root.querySelector("#library-clear"),
        count: root.querySelector("#library-count"),
        asOf: root.querySelector("#library-as-of"),
      };
      inst.q.value = libraryState.q;
      libraryState.instance = inst;
      bindLibrary(inst);
      RD.setTitle("Analysis notes · IDX Research Desk");
    },
    unmount() {
      const inst = libraryState.instance;
      if (inst) {
        inst.alive = false;
        if (inst.onDocuments) window.removeEventListener("rd:documents", inst.onDocuments);
      }
      libraryState.instance = null;
    },
  };

  /* ========================================================================
     Document workspace — #/doc/{id}
     ======================================================================== */

  const chatStore = new Map();
  let activeSession = null;

  function getChat(docId) {
    if (!chatStore.has(docId)) {
      chatStore.set(docId, {
        turns: [],
        seq: 0,
        draft: "",
        selection: null,
        tab: "document",
        hintKey: "",
      });
    }
    return chatStore.get(docId);
  }

  function docChips(doc) {
    const chips = [];
    if (doc.ticker) chips.push('<span class="chip chip--ticker">' + RD.esc(doc.ticker) + "</span>");
    if (doc.period || doc.year) {
      chips.push('<span class="chip">' + RD.esc([doc.period, doc.year].filter(Boolean).join(" ")) + "</span>");
    }
    if (!doc.ticker) chips.push('<span class="chip chip--kind">' + RD.esc(RD.humanState(doc.kind)) + "</span>");
    chips.push('<span class="chip chip--note">secondary note</span>');
    return chips.join("");
  }

  function workspaceShell(doc, inst) {
    const sections = asSectionList(doc);
    const updated = doc.updated_at ? RD.formatDate(doc.updated_at) : "—";
    const stale = isStale(doc);
    const title = doc.title || doc.id;
    const metaBits = [
      updated !== "—" ? "Updated " + updated : "",
      sections.length ? sections.length + " section" + (sections.length === 1 ? "" : "s") : "",
      doc.words ? RD.formatNumber(Number(doc.words)) + " words" : "",
      doc.bytes ? RD.formatBytes(doc.bytes) : "",
      doc.sha256 ? "sha " + String(doc.sha256).slice(0, 10) : "",
    ].filter(Boolean);

    return '<div class="doc-workspace" data-tab="document">' +
      '<header class="doc-head">' +
        '<nav class="doc-crumb" aria-label="Breadcrumb">' +
          '<a href="#/documents">Vault</a><span aria-hidden="true">/</span>' +
          '<span>' + RD.esc(doc.id) + "</span>" +
        "</nav>" +
        '<div class="doc-head__main">' +
          '<div class="doc-head__titles">' +
            '<p class="eyebrow"><span class="eyebrow__no">02</span>Document workspace</p>' +
            '<h1 class="doc-head__title">' + RD.esc(title) + "</h1>" +
            '<div class="doc-head__chips">' + docChips(doc) + "</div>" +
          "</div>" +
          '<div class="doc-head__actions">' +
            '<button type="button" class="button-quiet" data-action="print">Print</button>' +
            '<button type="button" class="button-quiet" data-action="export">Export .md</button>' +
            '<button type="button" class="button-quiet" data-action="copy-link">Copy link</button>' +
            '<a class="button-quiet" href="#/documents">Library</a>' +
          "</div>" +
        "</div>" +
        '<p class="doc-head__meta">' + RD.esc(metaBits.join(" · ")) + "</p>" +
        (stale ? '<p class="doc-stale" role="note"><b>Stale note</b> — the vault marks this analysis as behind the latest filing. Read the view, then verify the number against the filings.</p>' : "") +
      "</header>" +
      '<div class="doc-tabs" role="tablist" aria-label="Document workspace views">' +
        '<button type="button" role="tab" id="doc-tab-document" aria-controls="doc-panel-document" aria-selected="true" tabindex="0" data-tab="document">Document</button>' +
        '<button type="button" role="tab" id="doc-tab-ask" aria-controls="doc-panel-ask" aria-selected="false" tabindex="-1" data-tab="ask">Ask this document</button>' +
      "</div>" +
      '<div class="doc-layout">' +
        '<section class="doc-reader-panel" id="doc-panel-document" role="tabpanel" aria-labelledby="doc-tab-document" data-panel="document">' +
          '<nav class="doc-toc" aria-label="Table of contents">' +
            '<p class="doc-toc__title">Contents</p>' +
            '<ol class="doc-toc__list" id="doc-toc"></ol>' +
          "</nav>" +
          '<div class="doc-reader" id="doc-reader" tabindex="0" aria-label="Full document">' +
            '<div class="reading-progress" role="progressbar" aria-label="Reading progress" aria-valuemin="0" aria-valuemax="100" aria-valuenow="0"><span id="reading-progress"></span></div>' +
            '<div class="doc-body" id="doc-body"></div>' +
          "</div>" +
        "</section>" +
        '<aside class="doc-chat" id="doc-panel-ask" role="tabpanel" aria-labelledby="doc-tab-ask" data-panel="ask">' +
          chatShell(doc) +
        "</aside>" +
      "</div>" +
      '<div class="selection-toast" id="selection-toast" role="status" hidden>' +
        '<span class="selection-toast__label">Selection</span>' +
        '<span class="selection-toast__quote" id="selection-toast-quote"></span>' +
        '<button type="button" class="button-quiet" data-selection-attach>Ask about this</button>' +
        '<button type="button" class="selection-toast__close" data-selection-dismiss aria-label="Dismiss selection">×</button>' +
      "</div>" +
    "</div>";
  }

  function chatShell(doc) {
    const scope = doc.ticker
      ? doc.ticker + " · " + ([doc.period, doc.year].filter(Boolean).join(" ") || "latest")
      : RD.humanState(doc.kind);
    return '<header class="doc-chat__head">' +
        '<div class="doc-chat__headline">' +
          '<p class="doc-chat__kicker">Ask this document</p>' +
          '<h2 class="doc-chat__title">' + RD.esc(doc.title || doc.id) + "</h2>" +
          '<div class="doc-chat__scope"><span class="scope-chip">' + RD.esc(scope) + '</span>' +
            '<span class="scope-chip scope-chip--quiet">' + RD.esc(doc.id) + "</span></div>" +
        "</div>" +
        '<button type="button" class="text-button" data-action="new-thread">New thread</button>' +
      "</header>" +
      '<div class="doc-chat__scroll" id="doc-chat-scroll">' +
        '<div class="doc-chat__thread" id="doc-chat-thread"></div>' +
      "</div>" +
      '<div class="doc-chat__foot">' +
        '<div class="selection-chip" id="selection-chip" hidden>' +
          '<div class="selection-chip__head"><span>Ask about this passage</span>' +
            '<button type="button" class="selection-chip__remove" data-selection-remove aria-label="Remove attached selection">×</button></div>' +
          '<blockquote class="selection-chip__quote" id="selection-chip-quote"></blockquote>' +
          '<p class="selection-chip__where" id="selection-chip-where"></p>' +
        "</div>" +
        '<form class="chat-composer" id="doc-chat-form" novalidate>' +
          '<label class="sr-only" for="doc-chat-input">Question for this document</label>' +
          '<textarea id="doc-chat-input" rows="2" maxlength="' + MAX_QUESTION +
            '" placeholder="Ask about a section, a figure, or what the note leaves out…" required></textarea>' +
          '<div class="chat-composer__foot">' +
            '<p class="hint"><kbd>⌘/Ctrl</kbd><span aria-hidden="true">+</span><kbd>↵</kbd> to ask · <span id="doc-chat-count">0</span>/' + MAX_QUESTION + "</p>" +
            '<button type="submit" class="ask-button ask-button--compact" id="doc-chat-submit">' +
              '<span id="doc-chat-submit-label">Ask note</span><span class="ask-button__arrow" aria-hidden="true">→</span></button>' +
          "</div>" +
        "</form>" +
      "</div>" +
    "";
  }

  function asSectionList(doc) {
    return RD.asArray(doc && doc.sections).filter((section) => section && (section.id || section.title));
  }

  function frontmatterPanel(doc, parsed) {
    const meta = Object.assign({}, RD.objectOr(doc.frontmatter), RD.objectOr(parsed && parsed.meta));
    const rows = [];
    const skip = new Set(["content", "body"]);

    function push(label, value) {
      if (value === undefined || value === null || value === "") return;
      if (Array.isArray(value) && !value.length) return;
      rows.push([label, value]);
    }

    push("Kind", RD.humanState(doc.kind));
    push("Issuer", doc.ticker);
    push("Period", [doc.period, doc.year].filter(Boolean).join(" "));
    push("Updated", RD.formatDate(doc.updated_at));
    push("Path", doc.path);
    push("Words", doc.words ? RD.formatNumber(Number(doc.words)) : "");
    push("SHA-256", doc.sha256);
    push("Tags", RD.asArray(doc.tags));
    push("Aliases", RD.asArray(doc.aliases));
    Object.keys(meta).forEach((key) => {
      if (skip.has(key) || ["title", "ticker", "period", "year", "kind", "updated_at", "path", "sha256", "words", "tags", "aliases"].includes(key)) return;
      push(RD.humanKey(key), meta[key]);
    });

    if (!rows.length) return "";
    return '<details class="doc-frontmatter">' +
      '<summary>File metadata <span>' + rows.length + "</span></summary>" +
      '<dl class="frontmatter-grid">' + rows.map(([label, value]) =>
        "<dt>" + RD.esc(label) + "</dt><dd>" + valueHtml(value) + "</dd>"
      ).join("") + "</dl></details>";
  }

  function valueHtml(value) {
    if (Array.isArray(value)) {
      return value.map((item) => '<span class="chip chip--quiet">' + RD.esc(String(item)) + "</span>").join("");
    }
    if (value && typeof value === "object") {
      try {
        return "<code>" + RD.esc(JSON.stringify(value)) + "</code>";
      } catch (error) {
        return RD.esc(String(value));
      }
    }
    return RD.esc(String(value));
  }

  function outlineFor(doc, rendered) {
    if (rendered.headings && rendered.headings.length) {
      return rendered.headings.map((heading) => ({
        id: heading.id,
        title: heading.title,
        level: heading.level,
        line: heading.line,
      }));
    }
    return asSectionList(doc);
  }

  function renderToc(inst, outline) {
    const items = outline.filter((section) => Number(section.level) <= 3 || !section.level);
    if (!items.length) {
      inst.toc.innerHTML = '<li class="doc-toc__quiet">No headings in this note.</li>';
      return;
    }
    inst.toc.innerHTML = items.map((section) => {
      const level = Math.max(1, Math.min(3, Number(section.level) || 2));
      return '<li class="doc-toc__item doc-toc__item--' + level + '">' +
        '<button type="button" class="doc-toc__link" data-section="' + RD.escapeAttr(section.id) + '">' +
          RD.esc(section.title || section.id) +
        "</button></li>";
    }).join("");
    inst.tocById = new Map(items.map((section) => [String(section.id), section]));
  }

  function suggestionsFor(doc, outline) {
    const prompts = ["Summarise this note and cite the main sections."];
    outline
      .filter((section) => {
        const level = Number(section.level) || 2;
        if (level < 2 || level > 3) return false;
        if (!section.title) return false;
        return section.title.trim().toLowerCase() !== String(doc.title || "").trim().toLowerCase();
      })
      .slice(0, 3)
      .forEach((section) => {
        prompts.push("What does the note say in “" + section.title + "”?");
      });
    prompts.push("Which claims in this note are hypotheses rather than facts?");
    prompts.push("What risks does the note flag, and which are management-stated?");
    return Array.from(new Set(prompts)).slice(0, 6);
  }

  function emptyChat(inst, session) {
    const prompts = suggestionsFor(session.doc, session.outline || []);
    return '<div class="doc-chat__empty">' +
      '<p class="empty__kicker">Pinned note</p>' +
      '<h3 class="doc-chat__empty-title">Ask this note, section by section.</h3>' +
      '<p class="doc-chat__empty-body">The note is the primary context here. Note-derived claims carry a <b>NOTE</b> label and a section citation; filing-level values still come from the ledger and carry <b>FACT</b> or <b>MGMT</b>.</p>' +
      '<div class="suggestions" aria-label="Suggested questions">' +
        prompts.map((prompt) =>
          '<button type="button" class="suggestion" data-suggestion="' + RD.escapeAttr(prompt) + '">' +
            RD.esc(prompt) + "</button>"
        ).join("") +
      "</div>" +
      '<p class="doc-chat__quiet">This note is secondary evidence. Nothing here is a primary filing.</p>' +
    "</div>";
  }

  function chatTurnHtml(session, turn) {
    const result = turn.result;
    const scope = turn.scope || session.scope;
    const pending = turn.pending;
    const selection = turn.selection;
    const selectionHtml = selection
      ? '<div class="chat-selection"><span class="chat-selection__label">On the passage</span>' +
        '<blockquote>' + RD.esc(selection.quote) + "</blockquote>" +
        '<p class="chat-selection__where">' +
          RD.esc([selection.section, selection.line ? "line " + selection.line : ""].filter(Boolean).join(" · ")) +
        "</p></div>"
      : "";
    const dropped = !pending && selectionDropped(result)
      ? '<p class="chat-note chat-note--warn">The server dropped the attached selection: it was not an exact substring of the note. The answer below did not use it.</p>'
      : "";

    let answer = "";
    if (pending) {
      answer = RD.renderPending(turn.id, {
        label: "Reading this note",
        note: "Searching the pinned note first, then the fact sheets and captured filings. Note numbers can never become FACTS.",
      });
    } else {
      answer = RD.renderResult(result, {
        mode: "document",
        chipStyle: "round",
        missingActions: missingActionsFor(session, turn),
        actionsHtml: documentActionsFor(session, turn),
        coverage: false,
        matrix: false,
        footer: true,
      });
      answer += verificationPanel(session, turn);
    }

    return '<article class="chat-turn" data-turn-id="' + turn.id + '">' +
      '<header class="chat-turn__head">' +
        '<span class="chat-turn__no">Q.' + RD.pad2(turn.index) + "</span>" +
        '<p class="chat-turn__question">' + RD.esc(turn.question) + "</p>" +
        '<div class="chat-turn__meta">' +
          '<span class="meta-pill">' + RD.esc(scope.ticker || "note") + "</span>" +
          '<span>' + RD.esc(scope.periodLabel || scope.period || RD.humanState(session.doc.kind)) + "</span>" +
          '<time>' + RD.esc(RD.formatClock(turn.startedAt)) + "</time>" +
        "</div>" +
      "</header>" +
      selectionHtml + dropped +
      '<div class="chat-turn__body">' + answer + "</div>" +
    "</article>";
  }

  function selectionDropped(result) {
    if (!result) return false;
    if (result.selection_dropped === true) return true;
    if (!result.selection_echo) return false;
    const echo = result.selection_echo;
    if (typeof echo === "string") return /drop/i.test(echo);
    return echo.dropped === true || echo.status === "dropped" || echo.state === "dropped" || echo.ok === false ||
      echo.accepted === false;
  }

  function missingActionsFor(session, turn) {
    return function missingActions(entry) {
      const item = (entry && entry.item) || "this item";
      const question = "What is the filing-level position on " + item + " for " +
        (session.doc.ticker || session.doc.title) + "?";
      const href = RD.deskHref({
        q: question,
        ticker: session.doc.ticker || "",
        period: session.doc.period || "",
        year: session.doc.year || "",
        from: "doc",
        doc: session.doc.id,
      });
      const filingHref = RD.deskHref({
        q: "Which filings are captured for " + (session.doc.ticker || "this issuer") + " " +
          [session.doc.period, session.doc.year].filter(Boolean).join(" ") + ", and what does documents_status say?",
        ticker: session.doc.ticker || "",
        period: session.doc.period || "",
        year: session.doc.year || "",
        from: "doc",
        doc: session.doc.id,
      });
      return '<a class="button-quiet" href="' + RD.escapeAttr(href) + '">Ask the desk about this →</a>' +
        '<a class="button-quiet" href="' + RD.escapeAttr(filingHref) + '">Check filing status →</a>';
    };
  }

  function documentActionsFor(session, turn) {
    if (turn.pending) return "";
    const doc = session.doc;
    const escalate = RD.deskHref({
      q: turn.question,
      from: "doc",
      doc: doc.id,
    });
    return '<div class="chat-actions">' +
      '<button type="button" class="button-quiet" data-action="verify" data-turn="' + turn.id + '">' +
        "Verify against filings</button>" +
      '<a class="button-quiet" href="' + RD.escapeAttr(escalate) + '">Ask across companies →</a>' +
    "</div>";
  }

  function verificationPanel(session, turn) {
    const verify = turn.verify;
    if (!verify) return "";
    const noteClaim = verify.noteClaim || "";
    let body = "";
    if (verify.pending) {
      body = RD.renderPending("verify-" + turn.id, {
        label: "Checking primary filings",
        note: "Desk mode. The note statement is held aside; the filings answer is rendered separately and never overwrites the note view.",
      });
    } else {
      body = RD.renderResult(verify.result, {
        mode: "desk",
        chipStyle: "tagged",
        coverage: false,
      });
      body = body.replace(/data-retry/g, 'data-action="verify-retry" data-turn="' + turn.id + '"');
    }
    return '<section class="verify-panel" aria-label="Filings check">' +
      '<div class="verify-panel__head">' +
        '<span class="verify-panel__stamp">Filings check</span>' +
        '<p>Note view vs primary source · desk mode</p>' +
      "</div>" +
      '<blockquote class="verify-panel__claim"><span>Note statement</span>' + RD.esc(noteClaim) + "</blockquote>" +
      '<div class="verify-panel__body">' + body + "</div>" +
    "</section>";
  }

  function renderChat(inst, session) {
    if (!inst.alive) return;
    const thread = inst.chatThread;
    if (!thread) return;
    if (!session.turns.length) {
      thread.innerHTML = emptyChat(inst, session);
      updateChatBusy(inst, session);
      return;
    }
    thread.innerHTML = session.turns.map((turn) => chatTurnHtml(session, turn)).join("");
    updateChatBusy(inst, session);
    if (!session.keepScroll) {
      window.requestAnimationFrame(() => {
        if (inst.chatScroll) inst.chatScroll.scrollTop = inst.chatScroll.scrollHeight;
      });
    }
    session.keepScroll = false;
  }

  function updateChatBusy(inst, session) {
    if (!inst.chatSubmit) return;
    inst.chatSubmit.disabled = session.busy;
    inst.chatForm.setAttribute("aria-busy", String(session.busy));
    inst.chatSubmitLabel.textContent = session.busy ? "Reading…" : "Ask note";
  }

  function startChatTimer(inst, session) {
    if (session.timer) window.clearInterval(session.timer);
    session.timer = window.setInterval(() => {
      const pending = session.turns.find((turn) => turn.pending) ||
        session.turns.find((turn) => turn.verify && turn.verify.pending);
      if (!pending) {
        window.clearInterval(session.timer);
        session.timer = null;
        return;
      }
      const value = ((Date.now() - (pending.verify && pending.verify.pending
        ? pending.verify.startedAt
        : pending.startedAt)) / 1000).toFixed(1) + " s";
      const id = pending.verify && pending.verify.pending ? "verify-" + pending.id : pending.id;
      document.querySelectorAll('[data-elapsed-turn="' + id + '"]').forEach((node) => {
        node.textContent = value;
      });
    }, 100);
  }

  function stopChatTimer(session) {
    if (!session.timer) return;
    window.clearInterval(session.timer);
    session.timer = null;
  }

  // A run can outlive its view. If the user comes back to the same note before
  // the server answers, bring the new view up to date with the shared thread.
  function refreshActiveChat(session) {
    const active = activeSession;
    if (!active || !active.inst.alive || active.session === session) return;
    if (!active.session.doc || active.session.doc.id !== session.doc.id) return;
    active.session.turns = session.turns;
    active.session.seq = session.seq;
    renderChat(active.inst, active.session);
  }

  async function askDocument(inst, session, question, selection) {
    const text = String(question || "").trim();
    if (!text || session.busy) return;
    if (!text) {
      inst.chatInput.focus();
      RD.announce("Type a question before asking.");
      return;
    }

    session.seq += 1;
    session.busy = true;
    const turn = {
      id: session.seq,
      index: session.turns.length + 1,
      question: text,
      selection: selection || null,
      scope: session.scope,
      startedAt: Date.now(),
      pending: true,
      result: null,
      verify: null,
    };
    session.turns.push(turn);
    inst.chatInput.value = "";
    session.draft = "";
    updateChatCount(inst);
    session.selection = null;
    renderSelection(inst, session);
    renderChat(inst, session);
    startChatTimer(inst, session);
    RD.announce("Question filed against this note. Reading the note and the filings.");

    const payload = {
      mode: "document",
      document_id: session.doc.id,
      question: text,
      selection: selection
        ? { section: selection.section, line: selection.line, quote: selection.quote }
        : null,
      history: RD.historyFrom(session.turns.slice(0, -1)),
    };

    let result;
    try {
      result = await RD.postAsk(payload);
    } catch (error) {
      result = { status: "error", error: error && error.message ? error.message : "network error" };
    }
    turn.result = result;
    turn.pending = false;
    session.busy = false;
    stopChatTimer(session);
    renderChat(inst, session);
    updateChatBusy(inst, session);
    refreshActiveChat(session);
    RD.announceResult(result);
  }

  async function verifyAgainstFilings(inst, session, turn) {
    if (session.busy) return;
    const envelope = RD.objectOr(turn.result && turn.result.envelope);
    const claims = RD.asArray(envelope.claims).map((claim) => claim && claim.text).filter(Boolean);
    const noteClaim = claims.length
      ? claims.slice(0, 2).join(" ")
      : String(envelope.answer || "The note claim is not available.").slice(0, 700);
    const question = "Verify this note statement against primary filings and fact sheets only: \"" +
      noteClaim + "\". State the filing value or documents_status separately from the note's own view. " +
      "Do not treat the note as primary evidence and do not repeat a note number as fact.";

    session.busy = true;
    turn.verify = {
      pending: true,
      startedAt: Date.now(),
      noteClaim: noteClaim,
      result: null,
      question: question,
    };
    renderChat(inst, session);
    startChatTimer(inst, session);
    RD.announce("Checking the note statement against primary filings.");

    let result;
    try {
      result = await RD.postAsk({
        mode: "desk",
        question: question,
        ticker: session.doc.ticker || null,
        period: session.doc.period || null,
        year: session.doc.year || null,
        history: null,
      });
    } catch (error) {
      result = { status: "error", error: error && error.message ? error.message : "network error" };
    }
    turn.verify.pending = false;
    turn.verify.result = result;
    session.busy = false;
    stopChatTimer(session);
    renderChat(inst, session);
    refreshActiveChat(session);
    RD.announceResult(result);
  }

  function updateChatCount(inst) {
    if (inst.chatCount) inst.chatCount.textContent = String(inst.chatInput.value.length);
    if (inst.chatInput) {
      inst.chatInput.style.height = "auto";
      inst.chatInput.style.height = Math.min(inst.chatInput.scrollHeight, 190) + "px";
    }
  }

  function renderSelection(inst, session) {
    const chip = inst.selectionChip;
    if (!chip) return;
    if (!session.selection) {
      chip.hidden = true;
      return;
    }
    chip.hidden = false;
    inst.selectionChipQuote.textContent = session.selection.quote;
    inst.selectionChipWhere.textContent = [
      session.selection.section ? "§ " + session.selection.section : "",
      session.selection.line ? "line " + session.selection.line : "",
    ].filter(Boolean).join(" · ");
  }

  function setSelection(inst, session, selection) {
    if (!selection || !selection.quote) {
      session.selection = null;
    } else {
      session.selection = {
        section: selection.section || "",
        line: selection.line || null,
        quote: String(selection.quote).slice(0, 600),
      };
    }
    renderSelection(inst, session);
  }

  function captureSelection(inst, session) {
    const selection = window.getSelection();
    if (!selection || selection.isCollapsed) return;
    const text = selection.toString().replace(/\s+/g, " ").trim();
    if (text.length < 3) return;
    const anchor = selection.anchorNode;
    const element = anchor && (anchor.nodeType === 1 ? anchor : anchor.parentElement);
    if (!element || !inst.docBody.contains(element)) return;

    const sectionEl = element.closest(".doc-section");
    const lineEl = element.closest("[data-line]");
    const sectionId = sectionEl ? sectionEl.getAttribute("data-section") : "";
    const section = sectionId && session.tocById ? session.tocById.get(sectionId) : null;
    const line = lineEl ? Number(lineEl.getAttribute("data-line")) : (section ? Number(section.line) : null);
    const front = sectionEl && sectionEl.classList.contains("doc-section--front");
    setSelection(inst, session, {
      section: section ? section.title : (front ? "Document header" : ""),
      line: Number.isFinite(line) && line > 0 ? line : null,
      quote: text.slice(0, 600),
    });
    inst.selectionToastQuote.textContent = "“" + (text.length > 130 ? text.slice(0, 130) + "…" : text) + "”";
    inst.selectionToast.hidden = false;
    RD.announce("Passage captured. Choose Ask about this to attach it to your next question.");
  }

  function attachSelection(inst, session) {
    if (!session.selection) return;
    inst.selectionToast.hidden = true;
    if (RD.isMobile()) switchTab(inst, session, "ask");
    renderSelection(inst, session);
    inst.chatInput.focus();
    RD.announce("Passage attached. Ask your question.");
  }

  function clearNativeSelection() {
    const selection = window.getSelection();
    if (selection) {
      try {
        selection.removeAllRanges();
      } catch (error) { /* selection already gone */ }
    }
  }

  /* ------------------------------------------------------------------------
     Reader: TOC, scroll-spy, progress, anchors, citations
     ------------------------------------------------------------------------ */

  function renderReader(inst, session) {
    const doc = session.doc;
    const parsed = RD.markdown.parseFrontmatter(doc.content || "");
    const sections = asSectionList(doc);
    const sectionIds = {};
    sections.forEach((section) => {
      if (section.line) sectionIds[Number(section.line)] = String(section.id);
    });

    const rendered = RD.markdown.renderDocument(parsed.body, {
      sectionIds: sectionIds,
      startLine: parsed.bodyStart || 1,
      resolveWiki: true,
      images: true,
      links: true,
    });
    session.rendered = rendered;
    session.outline = outlineFor(doc, rendered);
    session.tocById = new Map(session.outline.map((section) => [String(section.id), section]));

    inst.docBody.innerHTML = frontmatterPanel(doc, parsed) + rendered.html;
    renderToc(inst, session.outline);
    bindReaderScroll(inst, session);
  }

  function bindReaderScroll(inst, session) {
    if (session.scrollHandler) {
      inst.reader.removeEventListener("scroll", session.scrollHandler);
      window.removeEventListener("scroll", session.scrollHandler);
    }
    let frame = 0;
    session.scrollHandler = () => {
      if (frame) return;
      frame = window.requestAnimationFrame(() => {
        frame = 0;
        updateReadingProgress(inst, session);
        updateScrollSpy(inst, session);
      });
    };
    inst.reader.addEventListener("scroll", session.scrollHandler, { passive: true });
    window.addEventListener("scroll", session.scrollHandler, { passive: true });
    window.requestAnimationFrame(() => {
      updateReadingProgress(inst, session);
      updateScrollSpy(inst, session);
    });
  }

  function updateReadingProgress(inst, session) {
    const container = inst.reader;
    if (!container || !inst.progress) return;
    let ratio = 0;
    if (RD.isMobile()) {
      const top = container.getBoundingClientRect().top + window.scrollY;
      const height = Math.max(1, container.scrollHeight - window.innerHeight);
      ratio = (window.scrollY - top + 120) / height;
    } else {
      const max = container.scrollHeight - container.clientHeight;
      ratio = max > 0 ? container.scrollTop / max : 0;
    }
    ratio = Math.max(0, Math.min(1, ratio));
    inst.progress.style.width = (ratio * 100).toFixed(2) + "%";
    inst.progress.parentElement.setAttribute("aria-valuenow", String(Math.round(ratio * 100)));
  }

  function updateScrollSpy(inst, session) {
    const headings = Array.from(inst.docBody.querySelectorAll(".doc-h"));
    if (!headings.length) return;
    const containerTop = RD.isMobile()
      ? 100
      : inst.reader.getBoundingClientRect().top;
    let active = headings[0];
    for (const heading of headings) {
      const top = heading.getBoundingClientRect().top;
      if (top - containerTop <= 130) active = heading;
      else break;
    }
    const id = active ? active.id : "";
    inst.toc.querySelectorAll(".doc-toc__link").forEach((link) => {
      const isActive = link.dataset.section === id;
      link.classList.toggle("is-active", isActive);
      if (isActive) link.setAttribute("aria-current", "location");
      else link.removeAttribute("aria-current");
    });
  }

  function goToSection(inst, session, reference, options) {
    const o = options || {};
    if (!reference && reference !== 0) return false;
    const section = RD.findSection(session.doc, reference);
    let targetId = section ? section.id : String(reference);
    if (!targetId && reference) targetId = RD.slugify(reference);
    let target = null;
    try {
      const ids = Array.from(inst.docBody.querySelectorAll("[id]"));
      target = ids.find((node) => node.id === targetId) || null;
    } catch (error) {
      target = null;
    }
    if (!target && section && section.line) {
      target = inst.docBody.querySelector('[data-line="' + section.line + '"]');
    }
    if (!target) {
      target = Array.from(inst.docBody.querySelectorAll(".doc-h")).find(
        (heading) => heading.textContent.replace(/#\s*$/, "").trim() === String(reference).trim()
      ) || null;
    }
    if (!target) {
      RD.announce("That section is not present in this note.");
      return false;
    }
    flashSection(target, o.quote);
    if (RD.isMobile()) {
      const top = target.getBoundingClientRect().top + window.scrollY - 112;
      window.scrollTo({ top: top, behavior: RD.prefersReducedMotion() ? "auto" : "smooth" });
    } else {
      const top = target.getBoundingClientRect().top - inst.reader.getBoundingClientRect().top +
        inst.reader.scrollTop - 18;
      inst.reader.scrollTo({ top: top, behavior: RD.prefersReducedMotion() ? "auto" : "smooth" });
    }
    try {
      history.replaceState(null, "", RD.docHref(session.doc.id, {
        section: section ? section.id : targetId,
        from: session.hintFrom || "",
        q: session.hintQuestion || "",
        quote: o.quote || "",
      }));
    } catch (error) { /* file:// or a locked URL still scrolls */ }
    if (RD.isMobile()) switchTab(inst, session, "document", { preserveScroll: true });
    return true;
  }

  function flashSection(target, quote) {
    const block = target.closest(".doc-section") || target;
    block.classList.add("is-cited");
    window.setTimeout(() => block.classList.remove("is-cited"), 3800);
    if (quote) highlightQuote(block, quote);
  }

  function highlightQuote(root, quote) {
    const needle = String(quote).replace(/\s+/g, " ").trim().toLowerCase();
    if (needle.length < 8) return;
    const walker = document.createTreeWalker(root, NodeFilter.SHOW_TEXT, {
      acceptNode(node) {
        if (!node.nodeValue || !node.nodeValue.trim()) return NodeFilter.FILTER_REJECT;
        if (node.parentElement && /^(SCRIPT|STYLE|MARK)$/.test(node.parentElement.tagName)) {
          return NodeFilter.FILTER_REJECT;
        }
        return NodeFilter.FILTER_ACCEPT;
      },
    });
    let node = walker.nextNode();
    while (node) {
      const value = node.nodeValue;
      const index = value.toLowerCase().indexOf(needle.slice(0, 120));
      if (index >= 0) {
        try {
          const range = document.createRange();
          range.setStart(node, index);
          range.setEnd(node, Math.min(value.length, index + needle.slice(0, 120).length));
          const mark = document.createElement("mark");
          mark.className = "cite-mark";
          range.surroundContents(mark);
          window.setTimeout(() => {
            if (mark.parentNode) {
              const text = document.createTextNode(mark.textContent);
              mark.replaceWith(text);
              if (text.parentNode) text.parentNode.normalize();
            }
          }, 6500);
          const first = root.querySelector(".cite-mark");
          if (first) {
            try {
              first.scrollIntoView({ block: "nearest", behavior: RD.prefersReducedMotion() ? "auto" : "smooth" });
            } catch (error) { /* the section is already in view */ }
          }
        } catch (error) {
          return;
        }
        return;
      }
      node = walker.nextNode();
    }
  }

  /* ------------------------------------------------------------------------
     Tabs
     ------------------------------------------------------------------------ */

  function switchTab(inst, session, tab, options) {
    const o = options || {};
    session.tab = tab;
    inst.workspace.dataset.tab = tab;
    inst.tabButtons.forEach((button) => {
      const active = button.dataset.tab === tab;
      button.setAttribute("aria-selected", String(active));
      button.tabIndex = active ? 0 : -1;
    });
    applyTabs(inst, session);
    if (RD.isMobile() && !o.preserveScroll && inst.workspace) {
      const top = inst.workspace.getBoundingClientRect().top + window.scrollY;
      window.scrollTo({ top: Math.max(0, top), behavior: "auto" });
    }
    if (tab === "ask" && inst.chatInput) {
      window.setTimeout(() => inst.chatInput.focus({ preventScroll: true }), 30);
    }
  }

  function applyTabs(inst, session) {
    const mobile = RD.isMobile();
    inst.panels.forEach((panel) => {
      const name = panel.dataset.panel;
      if (!mobile) {
        panel.hidden = false;
        return;
      }
      panel.hidden = name !== session.tab;
    });
    inst.tabButtons.forEach((button) => {
      button.setAttribute("aria-selected", String(button.dataset.tab === session.tab));
      button.tabIndex = button.dataset.tab === session.tab ? 0 : -1;
    });
  }

  /* ------------------------------------------------------------------------
     Workspace mount
     ------------------------------------------------------------------------ */

  async function mountWorkspace(root, route) {
    const docId = route.docId;
    const chat = getChat(docId);
    const hintKey = route.search || "";
    root.innerHTML = '<div class="doc-loading" aria-busy="true">' +
      '<p class="eyebrow"><span class="eyebrow__no">02</span>Document workspace</p>' +
      '<h1 class="doc-loading__title">Opening the note…</h1>' +
      '<p class="doc-loading__body">Reading ' + RD.esc(docId) + " from the analysis vault.</p>" +
      '<div class="skeleton-card skeleton-card--wide"><span></span><span></span><span></span></div>' +
    "</div>";

    let doc;
    try {
      doc = await RD.getDocument(docId);
    } catch (error) {
      if (!root.isConnected) return;
      root.innerHTML = workspaceErrorState(docId, error);
      bindWorkspaceError(root, docId);
      RD.setTitle("Note unavailable · IDX Research Desk");
      return;
    }
    if (!root.isConnected) return;

    const inst = {
      root: root,
      alive: true,
      doc: doc,
      route: route,
      selectionToast: null,
      tocById: new Map(),
      progressTimer: null,
    };
    const session = {
      doc: doc,
      chat: chat,
      scope: {
        ticker: doc.ticker || "",
        period: doc.period || "",
        periodLabel: [doc.period, doc.year].filter(Boolean).join(" ") ||
          RD.humanState(doc.kind),
        label: doc.ticker || RD.humanState(doc.kind),
      },
      hintFrom: route.params.get("from") || "",
      hintQuestion: route.params.get("q") || "",
      turns: chat.turns,
      seq: chat.seq,
      draft: chat.draft,
      selection: chat.selection,
      tab: chat.tab,
      busy: false,
      timer: null,
      rendered: null,
      outline: [],
      tocById: new Map(),
      alive: true,
    };
    activeSession = { inst: inst, session: session };

    root.innerHTML = workspaceShell(doc, inst);
    inst.workspace = root.querySelector(".doc-workspace");
    inst.docBody = root.querySelector("#doc-body");
    inst.reader = root.querySelector("#doc-reader");
    inst.toc = root.querySelector("#doc-toc");
    inst.progress = root.querySelector("#reading-progress");
    inst.tabButtons = Array.from(root.querySelectorAll(".doc-tabs [role='tab']"));
    inst.panels = Array.from(root.querySelectorAll("[data-panel]"));
    inst.selectionToast = root.querySelector("#selection-toast");
    inst.selectionToastQuote = root.querySelector("#selection-toast-quote");

    inst.chatScroll = root.querySelector("#doc-chat-scroll");
    inst.chatThread = root.querySelector("#doc-chat-thread");
    inst.chatForm = root.querySelector("#doc-chat-form");
    inst.chatInput = root.querySelector("#doc-chat-input");
    inst.chatCount = root.querySelector("#doc-chat-count");
    inst.chatSubmit = root.querySelector("#doc-chat-submit");
    inst.chatSubmitLabel = root.querySelector("#doc-chat-submit-label");
    inst.selectionChip = root.querySelector("#selection-chip");
    inst.selectionChipQuote = root.querySelector("#selection-chip-quote");
    inst.selectionChipWhere = root.querySelector("#selection-chip-where");

    try {
      renderReader(inst, session);
      renderChat(inst, session);
      renderSelection(inst, session);
      applyTabs(inst, session);
    } catch (error) {
      renderWorkspaceFailure(inst, session, error);
      return;
    }
    if (session.turns.some((turn) => turn.pending || (turn.verify && turn.verify.pending))) {
      startChatTimer(inst, session);
    }
    inst.tabButtons.forEach((button) => {
      button.addEventListener("click", () => switchTab(inst, session, button.dataset.tab));
      button.addEventListener("keydown", (event) => {
        if (event.key !== "ArrowLeft" && event.key !== "ArrowRight") return;
        event.preventDefault();
        const next = session.tab === "document" ? "ask" : "document";
        switchTab(inst, session, next);
        const target = inst.tabButtons.find((item) => item.dataset.tab === next);
        if (target) target.focus();
      });
    });

    // Route hints: prefill the composer when arriving from a desk citation or
    // escalation. Never auto-submit: a route must not spend a model run.
    const hintedQuestion = route.params.get("q") || "";
    if (hintedQuestion && chat.hintKey !== hintKey) {
      chat.hintKey = hintKey;
      if (route.params.get("from") === "desk") {
        // A desk citation opens a fresh document thread. Per-document history is
        // preserved for direct visits, but a desk answer must not leak into it.
        chat.turns = [];
        chat.seq = 0;
        chat.selection = null;
        session.turns = chat.turns;
        session.seq = 0;
        session.selection = null;
        renderSelection(inst, session);
        renderChat(inst, session);
      }
      inst.chatInput.value = hintedQuestion;
      chat.draft = hintedQuestion;
      updateChatCount(inst);
      const source = route.params.get("doc") || route.params.get("from");
      const notice = document.createElement("p");
      notice.className = "chat-note chat-note--hint";
      notice.textContent = source
        ? "Question carried from " + (route.params.get("from") === "desk" ? "the desk" : "another note") + ". Review it, then ask."
        : "Question loaded from the link. Review it, then ask.";
      inst.chatThread.prepend(notice);
    } else {
      inst.chatInput.value = chat.draft || "";
      updateChatCount(inst);
    }

    // Initial deep-link section.
    const initialSection = route.params.get("section");
    if (initialSection) {
      window.requestAnimationFrame(() => {
        goToSection(inst, session, initialSection, { quote: route.params.get("quote") || "" });
      });
    }

    bindWorkspaceEvents(inst, session);
    RD.setTitle((doc.title || doc.id) + " · IDX Research Desk");
  }

  function renderWorkspaceFailure(inst, session, error) {
    const message = error && error.message ? error.message : String(error || "unknown render error");
    inst.root.innerHTML = '<div class="doc-workspace doc-workspace--error">' +
      '<nav class="doc-crumb" aria-label="Breadcrumb"><a href="#/documents">Vault</a><span aria-hidden="true">/</span><span>' +
        RD.esc(session.doc.id) + "</span></nav>" +
      '<section class="library__state" aria-labelledby="doc-render-error-title">' +
        '<span class="panel__stamp">Reader error</span>' +
        '<h1 class="panel__title" id="doc-render-error-title">The note loaded, but the reader did not render.</h1>' +
        '<p class="panel__body">The API returned the note, but the client could not compose it. Nothing partial is shown: a half-rendered note is worse than an explicit state. The raw markdown is still available to export if the note opens again.</p>' +
        '<p class="panel__error">' + RD.esc(message) + "</p>" +
        '<div class="panel__actions">' +
          '<a class="button-quiet" href="#/documents">← Back to the library</a>' +
          '<button type="button" class="button-quiet" data-doc-retry>Retry</button>' +
          '<a class="button-quiet" href="' + RD.escapeAttr(RD.deskHref({
            q: "Open the note " + session.doc.id + " and summarise it.",
            from: "doc",
          })) + '">Ask the desk →</a>' +
        "</div>" +
      "</section>" +
    "</div>";
    bindWorkspaceError(inst.root, session.doc.id);
    RD.setTitle("Reader error · IDX Research Desk");
    RD.announce("The note loaded but the reader failed to render.");
  }

  function workspaceErrorState(docId, error) {
    const status = error && error.status;
    const message = error && error.message ? error.message : "The note could not be read.";
    const missing = status === 404 || /404|not found/i.test(message);
    const offline = /failed to fetch|network|offline/i.test(message);
    const stamp = missing ? "Missing note" : offline ? "Offline" : "Unreadable";
    const title = missing ? "This note is not on file." :
      offline ? "The vault is offline." : "This note could not be read.";
    const body = missing
      ? "The document index does not contain “" + docId + "”. A missing note is an explicit state, not a blank page. Return to the library or ask the desk about the issuer."
      : offline
        ? "The browser could not reach the local server. Start the app on 127.0.0.1:8765, then retry."
        : "The note exists but the reader could not load it. The server answered with an error; nothing partial is shown.";
    return '<div class="doc-workspace doc-workspace--error">' +
      '<nav class="doc-crumb" aria-label="Breadcrumb"><a href="#/documents">Vault</a><span aria-hidden="true">/</span><span>' +
        RD.esc(docId) + "</span></nav>" +
      '<section class="library__state" aria-labelledby="doc-error-title">' +
        '<span class="panel__stamp">' + RD.esc(stamp) + "</span>" +
        '<h1 class="panel__title" id="doc-error-title">' + RD.esc(title) + "</h1>" +
        '<p class="panel__body">' + RD.esc(body) + "</p>" +
        '<p class="panel__error">' + RD.esc(message) + "</p>" +
        '<div class="panel__actions">' +
          '<a class="button-quiet" href="#/documents">← Back to the library</a>' +
          '<button type="button" class="button-quiet" data-doc-retry>Retry</button>' +
          '<a class="button-quiet" href="' + RD.escapeAttr(RD.deskHref({
            q: "Is there an analysis note or a primary filing for " + docId + "?",
            from: "doc",
          })) + '">Ask the desk →</a>' +
        "</div>" +
      "</section>" +
    "</div>";
  }

  function bindWorkspaceError(root, docId) {
    root.addEventListener("click", (event) => {
      if (event.target.closest("[data-doc-retry]")) {
        RD.navigate("#/doc/" + encodeURIComponent(docId));
      }
    });
  }

  function bindWorkspaceEvents(inst, session) {
    const root = inst.root;

    // Tabs already bound by mountWorkspace.

    inst.chatInput.addEventListener("input", () => {
      session.draft = inst.chatInput.value;
      session.chat.draft = inst.chatInput.value;
      updateChatCount(inst);
    });

    inst.chatInput.addEventListener("keydown", (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        if (!session.busy) inst.chatForm.requestSubmit();
      }
      if (event.key === "Escape") {
        inst.selectionToast.hidden = true;
      }
    });

    inst.chatForm.addEventListener("submit", (event) => {
      event.preventDefault();
      if (session.busy) return;
      askDocument(inst, session, inst.chatInput.value, session.selection);
    });

    // Selection capture in the reader.
    inst.reader.addEventListener("mouseup", () => {
      window.setTimeout(() => captureSelection(inst, session), 10);
    });
    inst.reader.addEventListener("keyup", (event) => {
      if (event.shiftKey || event.key === "Shift") captureSelection(inst, session);
    });

    // Scroll-spy / progress already bound in renderReader.

    const media = window.matchMedia("(max-width: 959px)");
    session.mediaHandler = () => applyTabs(inst, session);
    if (media.addEventListener) media.addEventListener("change", session.mediaHandler);
    else media.addListener(session.mediaHandler);

    // Wiki links and document titles resolve once the index lands. Re-render
    // the reader in place (the API note is already on the page) without
    // disturbing the chat or the reader's scroll position.
    session.documentsHandler = () => {
      if (!inst.alive) return;
      const scrollTop = inst.reader ? inst.reader.scrollTop : 0;
      const tab = inst.workspace ? inst.workspace.dataset.tab : "document";
      try {
        renderReader(inst, session);
        inst.reader.scrollTop = scrollTop;
        const hash = window.location.hash || "";
        const queryIndex = hash.indexOf("?");
        const params = new URLSearchParams(queryIndex >= 0 ? hash.slice(queryIndex + 1) : "");
        const section = params.get("section");
        if (section && (!RD.isMobile() || tab === "document")) {
          goToSection(inst, session, section, { quote: params.get("quote") || "" });
        }
      } catch (error) {
        /* the reader already rendered without wiki links */
      }
    };
    window.addEventListener("rd:documents", session.documentsHandler);

    session.resizeHandler = RD.debounce(() => {
      updateReadingProgress(inst, session);
      updateScrollSpy(inst, session);
    }, 120);
    window.addEventListener("resize", session.resizeHandler);

    function turnFor(button) {
      const id = Number(button.getAttribute("data-turn"));
      return session.turns.find((turn) => Number(turn.id) === id) || null;
    }

    inst.chatThread.addEventListener("click", (event) => {
      const suggestion = event.target.closest("[data-suggestion]");
      if (suggestion) {
        inst.chatInput.value = suggestion.getAttribute("data-suggestion") || "";
        session.draft = inst.chatInput.value;
        session.chat.draft = inst.chatInput.value;
        updateChatCount(inst);
        inst.chatInput.focus();
        RD.announce("Suggested question loaded. Press Command or Control plus Enter to ask.");
        return;
      }

      const newThread = event.target.closest("[data-action='new-thread']");
      if (newThread) {
        if (session.busy) return;
        session.turns.length = 0;
        session.seq = 0;
        session.chat.turns = session.turns;
        session.chat.seq = 0;
        session.chat.draft = "";
        inst.chatInput.value = "";
        updateChatCount(inst);
        renderChat(inst, session);
        RD.announce("New thread. The previous document chat is cleared.");
        return;
      }

      const verify = event.target.closest("[data-action='verify']");
      if (verify) {
        const turn = turnFor(verify);
        if (turn) verifyAgainstFilings(inst, session, turn);
        return;
      }

      const retryVerify = event.target.closest("[data-action='verify-retry']");
      if (retryVerify) {
        const turn = turnFor(retryVerify);
        if (turn) verifyAgainstFilings(inst, session, turn);
        return;
      }

      const retry = event.target.closest("[data-retry]");
      if (retry) {
        const article = retry.closest(".chat-turn");
        const turn = article ? session.turns.find((item) => String(item.id) === article.dataset.turnId) : null;
        if (turn) askDocument(inst, session, turn.question, turn.selection);
        return;
      }

      const chip = event.target.closest(".cite-chip");
      if (chip) {
        const article = chip.closest(".chat-turn");
        const turn = article ? session.turns.find((item) => String(item.id) === article.dataset.turnId) : null;
        if (!turn) return;
        const inVerify = Boolean(chip.closest(".verify-panel"));
        const sourceResult = inVerify && turn.verify ? turn.verify.result : turn.result;
        const claimIndex = Number(chip.dataset.claim);
        const citeIndex = Number(chip.dataset.cite);
        const citation = RD.citations.from(sourceResult, claimIndex, citeIndex);
        if (!citation) return;
        const citationDoc = RD.citations.document(citation);
        if (citationDoc && citationDoc !== session.doc.id) {
          RD.navigate(RD.docHref(citationDoc, {
            section: citation.section || citation.line || "",
            from: "doc",
            q: inVerify && turn.verify ? turn.verify.question : turn.question,
          }));
          return;
        }
        if (citationDoc) {
          goToSection(inst, session, citation.section || citation.line, { quote: citation.quote || "" });
          return;
        }
        const claims = RD.asArray(sourceResult && sourceResult.envelope && sourceResult.envelope.claims);
        const claim = claims[claimIndex];
        const contextTurn = inVerify
          ? { question: turn.verify.question, result: sourceResult, scope: { ticker: session.doc.ticker || "desk" } }
          : turn;
        RD.inspector.open({
          turn: contextTurn,
          claim: claim,
          claimIndex: claimIndex,
          citation: citation,
          number: Number(chip.dataset.n),
          trigger: chip,
        });
        return;
      }
    });

    // Reader: TOC, anchors, selection toast.
    inst.toc.addEventListener("click", (event) => {
      const link = event.target.closest(".doc-toc__link");
      if (!link) return;
      goToSection(inst, session, link.dataset.section);
    });

    root.addEventListener("click", (event) => {
      const bodyLink = event.target.closest(".doc-body a[href]");
      if (bodyLink) {
        const href = bodyLink.getAttribute("href") || "";
        if (href.charAt(0) === "#" && href.indexOf("#/") !== 0) {
          if (href.length > 1) {
            event.preventDefault();
            goToSection(inst, session, href.slice(1));
          }
          return;
        }
        if (!/^(https?:)?\/\//i.test(href) && /\.md(?:#.*)?$/i.test(href)) {
          event.preventDefault();
          const target = href.split("#")[0].split("/").pop().replace(/\.md$/i, "");
          const resolved = RD.documents.resolve(target);
          if (resolved) {
            RD.navigate(RD.docHref(resolved.id, {
              section: resolved.section || "",
              from: "doc",
            }));
          } else {
            RD.announce("That linked note is not in the vault.");
          }
          return;
        }
      }

      const anchor = event.target.closest(".heading-anchor");
      if (anchor) {
        event.preventDefault();
        const id = anchor.getAttribute("data-anchor");
        const link = window.location.origin + window.location.pathname +
          RD.docHref(session.doc.id, { section: id });
        RD.copyText(link).then(() => {
          RD.announce("Section link copied.");
        }).catch(() => {
          RD.announce("Could not copy the link. The section address is in the bar.");
        });
        try {
          history.replaceState(null, "", RD.docHref(session.doc.id, { section: id }));
        } catch (error) { /* the reader still scrolls */ }
        return;
      }

      if (event.target.closest("[data-selection-attach]")) {
        attachSelection(inst, session);
        clearNativeSelection();
        return;
      }
      if (event.target.closest("[data-selection-dismiss]")) {
        inst.selectionToast.hidden = true;
        clearNativeSelection();
        return;
      }
      if (event.target.closest("[data-selection-remove]")) {
        setSelection(inst, session, null);
        session.chat.selection = null;
        return;
      }

      const action = event.target.closest("[data-action]");
      if (!action) return;
      const name = action.getAttribute("data-action");
      if (name === "print") {
        window.print();
        return;
      }
      if (name === "export") {
        exportNote(session.doc);
        return;
      }
      if (name === "copy-link") {
        RD.copyText(window.location.href).then(() => {
          RD.announce("Deep link copied.");
        }).catch(() => RD.announce("Could not copy the deep link."));
        return;
      }
      if (name === "new-thread") {
        if (session.busy) return;
        session.turns.length = 0;
        session.seq = 0;
        session.chat.turns = session.turns;
        session.chat.seq = 0;
        session.chat.draft = "";
        inst.chatInput.value = "";
        updateChatCount(inst);
        renderChat(inst, session);
        RD.announce("New thread started. The previous document chat is cleared.");
      }
    });
  }

  function exportNote(doc) {
    try {
      const blob = new Blob([doc.content || ""], { type: "text/markdown;charset=utf-8" });
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = String(doc.id || "note") + ".md";
      document.body.append(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      RD.announce("Markdown export downloaded.");
    } catch (error) {
      RD.announce("The export could not be prepared.");
    }
  }

  /* ------------------------------------------------------------------------
     Registration
     ------------------------------------------------------------------------ */

  function unmountWorkspace() {
    const active = activeSession;
    if (!active) return;
    active.inst.alive = false;
    const session = active.session;
    session.alive = false;
    stopChatTimer(session);
    const media = window.matchMedia("(max-width: 959px)");
    if (session.mediaHandler) {
      if (media.removeEventListener) media.removeEventListener("change", session.mediaHandler);
      else media.removeListener(session.mediaHandler);
    }
    if (session.resizeHandler) window.removeEventListener("resize", session.resizeHandler);
    if (session.documentsHandler) window.removeEventListener("rd:documents", session.documentsHandler);
    if (session.scrollHandler) {
      window.removeEventListener("scroll", session.scrollHandler);
      if (active.inst.reader) active.inst.reader.removeEventListener("scroll", session.scrollHandler);
    }
    // Persist per-document chat state and draft.
    session.chat.turns = session.turns;
    session.chat.seq = session.seq;
    session.chat.draft = active.inst.chatInput ? active.inst.chatInput.value : session.draft;
    session.chat.selection = session.selection;
    session.chat.tab = session.tab;
    activeSession = null;
  }

  RD.views.documents = libraryView;
  RD.views.workspace = {
    mount(root, route) {
      activeSession = null;
      mountWorkspace(root, route).catch((error) => {
        if (!root.isConnected) return;
        root.innerHTML = workspaceErrorState(route.docId, error);
        bindWorkspaceError(root, route.docId);
        RD.setTitle("Note unavailable · IDX Research Desk");
      });
    },
    unmount() {
      unmountWorkspace();
    },
  };
})(window);
