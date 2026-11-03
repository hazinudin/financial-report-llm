/* IDX Research Desk — wide-scope desk (#/desk).
 *
 * The existing single-page chat, moved behind a hash route. It answers over the
 * supported set only, asks for optional issuer/period focus, renders the shared
 * answer envelope, a cited comparison matrix when the answer carries one, and
 * never presents a ranking.
 */

"use strict";

(function (global) {
  const RD = global.RD;
  if (!RD) return;

  const MAX_QUESTION = 2000;

  const deskState = {
    turns: [],
    seq: 0,
    busy: false,
    ticker: "",
    period: "",
    draft: "",
    timer: null,
    hintKey: "",
    sourceDoc: "",
    instance: null,
  };

  function deskShell() {
    return '<div class="shell shell--desk">' +
      '<aside class="rail rail--desk" aria-label="Coverage and desk controls">' +
        '<section class="rail__block" aria-labelledby="desk-coverage-heading">' +
          '<div class="rail__label"><h2 id="desk-coverage-heading">Coverage</h2><span class="rail__no">A</span></div>' +
          '<dl class="stats" id="desk-coverage-stats">' +
            '<div><dt>Issuers</dt><dd data-stat="issuers">—</dd></div>' +
            '<div><dt>Periods</dt><dd data-stat="periods">—</dd></div>' +
            '<div><dt>Op. sets</dt><dd data-stat="operational">—</dd></div>' +
          "</dl>" +
          '<p class="as-of" id="desk-as-of"><span class="status-dot" data-state="loading" aria-hidden="true"></span>' +
            '<span id="desk-as-of-text">Reading coverage…</span></p>' +
          '<button class="text-button text-button--rail" id="desk-coverage-retry" type="button" hidden>Retry coverage feed</button>' +
        "</section>" +

        '<section class="rail__block rail__block--scope" aria-labelledby="desk-scope-heading">' +
          '<div class="rail__label"><h2 id="desk-scope-heading">Focus</h2><span class="rail__no">B</span></div>' +
          '<label class="field" for="ticker-select"><span class="field__label">Issuer</span>' +
            '<span class="select-wrap"><select id="ticker-select" disabled><option value="">All supported</option></select></span></label>' +
          '<label class="field" for="period-select"><span class="field__label">Period</span>' +
            '<span class="select-wrap"><select id="period-select" disabled><option value="">All periods</option></select></span></label>' +
          '<p class="field-note" id="scope-note">Focus is optional, not a filter. A comparison may name other supported tickers.</p>' +
        "</section>" +

        '<section class="rail__block" aria-labelledby="desk-prompts-heading">' +
          '<div class="rail__label"><h2 id="desk-prompts-heading">Start here</h2><span class="rail__no">C</span></div>' +
          '<ul class="prompts" id="desk-prompts">' +
            '<li><button type="button" data-question="Compare Q2 2026 coal production across ITMG, PTBA, PTPS and ESSA. Name every missing cell and its state."><span class="prompt__q">Compare Q2 coal production</span><span class="prompt__tag">Comparison</span></button></li>' +
            '<li><button type="button" data-question="Compare revenue growth and gross margin for MAPA and TAPG in Q2 2026. State the supported-set coverage and any not-applicable metric."><span class="prompt__q">Compare MAPA / TAPG margins</span><span class="prompt__tag">Comparison</span></button></li>' +
            '<li><button type="button" data-question="What do the analysis notes say about the coal sector in 2H 2026? Keep the answer to supported names and cite the notes."><span class="prompt__q">Coal sector notes, 2H 2026</span><span class="prompt__tag">Sector</span></button></li>' +
            '<li><button type="button" data-ticker="PTBA" data-question="What was PTBA\'s Q2 2026 coal production? If it is missing, say exactly why."><span class="prompt__q">PTBA Q2 coal production</span><span class="prompt__tag">Missing data</span></button></li>' +
            '<li><button type="button" data-ticker="BBCA" data-question="What was BBCA\'s gross margin in Q2 2026?"><span class="prompt__q">BBCA gross margin</span><span class="prompt__tag">Refusal</span></button></li>' +
            '<li><button type="button" data-ticker="ITMG" data-question="What was ITMG\'s single-quarter revenue in Q2 2026, and how was it derived?"><span class="prompt__q">ITMG Q2 revenue, single quarter</span><span class="prompt__tag">Derivation</span></button></li>' +
          "</ul>" +
        "</section>" +

        '<div class="rail__foot">' +
          '<p class="version">Supported names only · not an IDX-wide sector screen</p>' +
        "</div>" +
      "</aside>" +

      '<main class="desk desk--wide" id="desk-main">' +
        '<header class="masthead">' +
          '<p class="eyebrow"><span class="eyebrow__no">04</span>The research desk</p>' +
          '<div class="masthead__line">' +
            '<h1 class="title">Research desk</h1>' +
            '<p class="edition">Vol. 0 · Jakarta · <span id="desk-edition-date">2026</span></p>' +
          "</div>" +
          '<div class="coverage-line">' +
            '<span class="status-dot" data-desk-feed-dot data-state="loading" aria-hidden="true"></span>' +
            '<p id="desk-coverage-line">Wiring the coverage feed…</p>' +
            '<span class="coverage-line__rule" aria-hidden="true"></span>' +
            '<span class="coverage-line__tag">audited answers only</span>' +
          "</div>" +
          '<p class="standfirst">Comparison, sector narrative and cross-document synthesis over the supported set. Every figure is audited, every note claim is labelled, and coverage is what was actually called — never “the sector”.</p>' +
        "</header>" +

        '<section class="composer-section" aria-labelledby="desk-composer-heading">' +
          '<div class="section-label">' +
            '<span class="section-no">05</span><h2 id="desk-composer-heading">Enquiry</h2>' +
            '<span class="section-rule" aria-hidden="true"></span>' +
          "</div>" +
          '<div class="desk-source" id="desk-source" hidden></div>' +
          '<form class="composer" id="ask-form" novalidate>' +
            '<label class="sr-only" for="composer-input">Your question for the desk</label>' +
            '<textarea id="composer-input" name="question" rows="3" maxlength="' + MAX_QUESTION +
              '" placeholder="Compare issuers, ask for a sector read, or test a missing disclosure…" required></textarea>' +
            '<div class="composer__foot">' +
              '<p class="hint"><kbd>⌘/Ctrl</kbd><span aria-hidden="true">+</span><kbd>↵</kbd> to file · <span id="char-count">0</span>/' + MAX_QUESTION + "</p>" +
              '<button type="submit" class="ask-button" id="ask-button"><span id="ask-button-label">File question</span>' +
                '<span class="ask-button__arrow" aria-hidden="true">→</span></button>' +
            "</div>" +
          "</form>" +
          '<p class="desk-scope-line" id="desk-scope-line"></p>' +
        "</section>" +

        '<section class="record" aria-labelledby="desk-record-heading">' +
          '<div class="section-label section-label--record">' +
            '<span class="section-no">06</span><h2 id="desk-record-heading">Record</h2>' +
            '<span class="section-rule" aria-hidden="true"></span>' +
            '<button id="clear-thread" class="text-button" type="button" disabled>Clear desk</button>' +
          "</div>" +
          '<div class="thread" id="thread"></div>' +
        "</section>" +

        '<footer class="desk-foot">' +
          '<p>IDX Research Desk · fact sheets, captured releases, analysis notes</p>' +
          '<p class="desk-foot__aside">No ranking, no recommendation. Refusal is a feature.</p>' +
        "</footer>" +
      "</main>" +
    "</div>";
  }

  function emptyDesk() {
    return '<div class="empty" id="empty-state">' +
      '<div class="empty__folio" aria-hidden="true">00</div>' +
      '<div class="empty__copy">' +
        '<p class="empty__kicker">Awaiting first enquiry</p>' +
        '<h3 class="empty__title">Nothing on file yet.</h3>' +
        '<p class="empty__body">Leave the desk open or pin an issuer, then ask a comparison or sector question. The record holds the question, the claims, the cited matrix and the audit verdict.</p>' +
        '<p class="empty__key"><span><b>FACT</b> tool ledger</span><span><b>MGMT</b> document quote</span>' +
          '<span><b>NOTE</b> analysis note</span><span><b>HYP</b> labelled inference</span></p>' +
      "</div></div>";
  }

  function deskScope(inst) {
    if (!deskState.ticker) {
      return { ticker: null, period: null, year: null, periodLabel: "any period", label: "All supported" };
    }
    const match = /^(Q[1-4])\s+(\d{4})$/.exec(deskState.period || "");
    return {
      ticker: deskState.ticker,
      period: match ? match[1] : null,
      year: match ? Number(match[2]) : null,
      periodLabel: deskState.period || "latest",
      label: deskState.ticker,
    };
  }

  function companyFor(ticker) {
    return RD.asArray(RD.state.companies).find((company) => company.ticker === ticker) || null;
  }

  function setOption(select, label, value, disabled) {
    select.innerHTML = "";
    const option = document.createElement("option");
    option.value = value || "";
    option.textContent = label;
    select.append(option);
    select.disabled = disabled !== false;
  }

  function populateDeskTickers(inst) {
    const companies = RD.asArray(RD.state.companies);
    const previous = deskState.ticker;
    inst.ticker.innerHTML = "";
    const all = document.createElement("option");
    all.value = "";
    all.textContent = "All supported";
    inst.ticker.append(all);

    companies.forEach((company) => {
      const periods = RD.asArray(company.periods);
      const option = document.createElement("option");
      option.value = company.ticker;
      option.textContent = company.ticker + " — " + periods.length + " period" + (periods.length === 1 ? "" : "s");
      inst.ticker.append(option);
    });

    deskState.ticker = companies.some((company) => company.ticker === previous) ? previous : "";
    inst.ticker.value = deskState.ticker;
    inst.ticker.disabled = companies.length === 0;
    populateDeskPeriods(inst);
  }

  function populateDeskPeriods(inst) {
    const company = companyFor(deskState.ticker);
    inst.period.innerHTML = "";
    if (!company) {
      deskState.period = "";
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "All periods";
      inst.period.append(option);
      inst.period.disabled = true;
      updateDeskScopeNote(inst);
      return;
    }
    const periods = RD.asArray(company.periods);
    if (!periods.length) {
      deskState.period = "";
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No periods";
      inst.period.append(option);
      inst.period.disabled = true;
      updateDeskScopeNote(inst);
      return;
    }
    periods.forEach((period) => {
      const option = document.createElement("option");
      option.value = period;
      option.textContent = period;
      inst.period.append(option);
    });
    inst.period.disabled = false;
    if (!periods.includes(deskState.period)) deskState.period = periods[periods.length - 1];
    inst.period.value = deskState.period;
    updateDeskScopeNote(inst);
  }

  function updateDeskScopeNote(inst) {
    if (!inst.scopeNote) return;
    const company = companyFor(deskState.ticker);
    const scope = deskScope(inst);
    inst.scopeLine.textContent = scope.ticker
      ? "Focus: " + scope.ticker + " " + (scope.period || "latest") + ". A wider comparison may still name other supported issuers."
      : "Open scope: the question decides the issuers. The answer states the coverage it actually used.";
    if (!company) {
      inst.scopeNote.textContent = "Open scope. The question decides the issuer; answers may say unsupported.";
      return;
    }
    const operational = RD.asArray(company.operational_periods).includes(deskState.period);
    inst.scopeNote.textContent = company.ticker + " " + (deskState.period || "latest") + ". Financials: " +
      (company.financials || "unknown") + " · notes: " + (company.notes || "unknown") + ". " +
      (operational ? "Operational release captured." : "No operational document captured for this period.");
  }

  function renderDeskCoverage(inst) {
    const stats = RD.coverage.stats();
    const asOf = RD.formatDate(stats.asOf);
    const companies = RD.asArray(RD.state.companies);

    if (inst.statIssuers) inst.statIssuers.textContent = String(stats.issuers || "—");
    if (inst.statPeriods) inst.statPeriods.textContent = String(stats.periods || "—");
    if (inst.statOperational) inst.statOperational.textContent = String(stats.operational || "—");
    if (inst.editionDate) inst.editionDate.textContent = asOf;

    const dot = inst.root.querySelector("[data-desk-feed-dot]");
    if (RD.state.coverageState === "error") {
      if (dot) dot.dataset.state = "error";
      inst.coverageLine.textContent = "Offline — coverage unavailable. The desk cannot state the supported set.";
      inst.asOfText.textContent = "Coverage feed unreachable.";
      inst.coverageRetry.hidden = false;
      setOption(inst.ticker, "Coverage unavailable", "", true);
      setOption(inst.period, "Coverage unavailable", "", true);
      if (inst.scopeNote) inst.scopeNote.textContent = "The scope pickers return when the coverage feed answers.";
      return;
    }

    if (RD.state.coverageState === "loading" || !companies.length) {
      if (dot) dot.dataset.state = "loading";
      inst.coverageLine.textContent = "Wiring the coverage feed…";
      inst.asOfText.textContent = "Reading coverage…";
      return;
    }

    if (dot) dot.dataset.state = "ready";
    inst.asOfText.textContent = "as of " + asOf;
    inst.coverageLine.textContent =
      companies.length + " supported issuers · " + stats.periods + " fact-sheet periods · " +
      stats.operational + " operational " + (stats.operational === 1 ? "set" : "sets") +
      " · not IDX-wide · as of " + asOf;
    inst.coverageRetry.hidden = true;
    populateDeskTickers(inst);

    const pending = deskState.pendingFocus;
    if (pending && pending.ticker && companyFor(pending.ticker)) {
      deskState.pendingFocus = null;
      deskState.ticker = pending.ticker;
      const periods = RD.asArray(companyFor(pending.ticker).periods);
      deskState.period = periods.includes(pending.period) ? pending.period : "";
      populateDeskPeriods(inst);
      inst.ticker.value = pending.ticker;
      inst.period.value = deskState.period;
      updateDeskScopeNote(inst);
    }
  }

  function citedNotes(result, question) {
    const seen = new Map();
    const claims = RD.asArray(result && result.envelope && result.envelope.claims);
    claims.forEach((claim) => {
      RD.asArray(claim && claim.citations).forEach((citation) => {
        const id = RD.citations.document(citation);
        if (!id) return;
        const section = citation.section || "";
        const key = id + "|" + section;
        if (!seen.has(key)) {
          seen.set(key, { id: id, section: section, line: citation.line || "", quote: citation.quote || "" });
        }
      });
    });
    if (!seen.size) return "";
    const items = Array.from(seen.values()).map((entry) => {
      const title = RD.documents.title(entry.id);
      const href = RD.docHref(entry.id, {
        section: entry.section || entry.line || "",
        from: "desk",
        q: question,
      });
      return '<li><a href="' + RD.escapeAttr(href) + '">' + RD.esc(title) +
        (entry.section ? '<span class="cited-note__section">' + RD.esc(entry.section) + "</span>" : "") +
        '<span class="cited-note__arrow" aria-hidden="true">→</span></a></li>';
    }).join("");
    return '<section class="cited-notes" aria-label="Cited notes">' +
      '<p class="cited-notes__label">Cited notes <span>opens the reader at the cited section</span></p>' +
      "<ul>" + items + "</ul></section>";
  }

  function turnHtml(turn) {
    const scope = turn.scope || { ticker: null, periodLabel: "any period" };
    const pending = turn.pending;
    let body = "";
    if (pending) {
      body = RD.renderPending(turn.id, {
        label: "Reading filings and notes",
        note: "Checking the fact sheets, captured releases and analysis notes for " + RD.scopeText(scope) +
          ". Comparisons return a cited matrix; missing cells are states, never zeros.",
      });
    } else {
      body = RD.renderResult(turn.result, {
        mode: "desk",
        chipStyle: "tagged",
        coverage: true,
        matrix: true,
        actions: function deskActions(result) {
          return citedNotes(result, turn.question);
        },
      });
    }
    body += traceMatrixHtml(turn);
    return '<article class="turn" data-turn-id="' + turn.id + '">' +
      '<header class="turn__head">' +
        '<span class="turn__no">Q.' + RD.pad2(turn.index) + "</span>" +
        '<p class="turn__question">' + RD.esc(turn.question) + "</p>" +
        '<div class="turn__meta">' +
          '<span class="meta-pill">' + RD.esc(scope.ticker || "All supported") + "</span>" +
          '<span class="turn__scope">' + RD.esc(scope.ticker
            ? (scope.periodLabel || scope.period || "latest")
            : "any period") + "</span>" +
          '<time class="turn__time">' + RD.esc(RD.formatClock(turn.startedAt)) + "</time>" +
        "</div>" +
      "</header>" +
      '<div class="turn__body">' + body + "</div>" +
    "</article>";
  }

  function traceMatrixHtml(turn) {
    if (RD.asArray(turn.traceMatrices).length) {
      return turn.traceMatrices.map((matrix) => RD.renderComparison(matrix)).join("");
    }
    if (turn.matrixState === "loading") {
      return '<p class="matrix-trace-note" role="status">Reading the cited comparison matrix from the run trace…</p>';
    }
    if (turn.matrixState === "missing") {
      return '<p class="matrix-trace-note">The run trace held no comparison matrix. The prose and coverage above are unchanged.</p>';
    }
    if (turn.matrixState === "error") {
      return '<p class="matrix-trace-note">The comparison matrix could not be read from the trace. The prose and coverage above are unchanged.</p>';
    }
    return "";
  }

  function needsTraceMatrix(result) {
    if (!result || RD.resultStatus(result) !== "answered") return false;
    const calls = RD.asArray(result.tool_calls).map((call) =>
      typeof call === "string" ? call : String(call && (call.tool_name || call.tool) || "")
    );
    const coverage = (result.envelope && result.envelope.coverage) || result.coverage || {};
    const compared = RD.asArray(coverage.compared_tickers);
    if (!calls.includes("compare_supported") && compared.length < 2) return false;
    return RD.answer.collectComparisons({
      envelope: result.envelope,
      comparison: result.comparison,
      tool_calls: result.tool_calls,
    }).length === 0;
  }

  async function loadTraceMatrices(inst, turn) {
    const traceId = turn.result && turn.result.trace_id;
    if (!traceId) {
      turn.matrixState = "missing";
      renderDeskThread(inst);
      return;
    }
    try {
      const trace = await RD.loadTrace(traceId);
      turn.traceMatrices = RD.comparisonsFromTrace(trace);
      turn.matrixState = turn.traceMatrices.length ? "ready" : "missing";
    } catch (error) {
      turn.matrixState = "error";
    }
    if (inst.alive) {
      const scrollY = window.scrollY;
      renderDeskThread(inst);
      window.scrollTo(0, scrollY);
    }
    if (activeDesk && activeDesk !== inst && activeDesk.alive) {
      renderDeskThread(activeDesk);
    }
  }

  function renderDeskThread(inst, scrollToId) {
    if (!inst.alive) return;
    if (!deskState.turns.length) {
      inst.thread.innerHTML = emptyDesk();
      updateDeskBusy(inst);
      return;
    }
    inst.thread.innerHTML = deskState.turns.map(turnHtml).join("");
    updateDeskBusy(inst);
    if (scrollToId !== undefined) {
      const node = inst.thread.querySelector('[data-turn-id="' + scrollToId + '"]');
      if (node) {
        try {
          node.scrollIntoView({ behavior: RD.prefersReducedMotion() ? "auto" : "smooth", block: "start" });
        } catch (error) {
          node.scrollIntoView();
        }
      }
    }
  }

  function updateDeskBusy(inst) {
    if (!inst.askButton) return;
    inst.askButton.disabled = deskState.busy;
    inst.form.setAttribute("aria-busy", String(deskState.busy));
    inst.askLabel.textContent = deskState.busy ? "Filing…" : "File question";
    inst.clear.disabled = deskState.busy || deskState.turns.length === 0;
  }

  function startDeskTimer(inst) {
    if (deskState.timer) window.clearInterval(deskState.timer);
    deskState.timer = window.setInterval(() => {
      const pending = deskState.turns.find((turn) => turn.pending);
      if (!pending) {
        window.clearInterval(deskState.timer);
        deskState.timer = null;
        return;
      }
      const value = ((Date.now() - pending.startedAt) / 1000).toFixed(1) + " s";
      document.querySelectorAll('[data-elapsed-turn="' + pending.id + '"]').forEach((node) => {
        node.textContent = value;
      });
    }, 100);
  }

  function stopDeskTimer() {
    if (!deskState.timer) return;
    window.clearInterval(deskState.timer);
    deskState.timer = null;
  }

  function updateCharCount(inst) {
    inst.charCount.textContent = String(inst.input.value.length);
  }

  async function askDesk(inst, rawQuestion, scopeOverride) {
    const question = String(rawQuestion || "").trim();
    if (!question) {
      inst.form.classList.add("is-invalid");
      inst.input.focus();
      RD.announce("Type a question before filing.");
      return;
    }
    if (deskState.busy) return;

    inst.form.classList.remove("is-invalid");
    const scope = scopeOverride || deskScope(inst);
    deskState.seq += 1;
    const turn = {
      id: deskState.seq,
      index: deskState.turns.length + 1,
      question: question,
      scope: scope,
      startedAt: Date.now(),
      pending: true,
      result: null,
    };
    deskState.turns.push(turn);
    deskState.busy = true;
    deskState.draft = "";
    inst.input.value = "";
    updateCharCount(inst);
    renderDeskThread(inst, turn.id);
    startDeskTimer(inst);
    RD.announce("Question filed. The desk is reading filings and notes.");

    const payload = {
      mode: "desk",
      question: question,
      ticker: scope.ticker || null,
      period: scope.period || null,
      year: typeof scope.year === "number" ? scope.year : null,
      history: RD.historyFrom(deskState.turns.slice(0, -1)),
    };

    let result;
    try {
      result = await RD.postAsk(payload);
    } catch (error) {
      result = { status: "error", error: error && error.message ? error.message : "network error" };
    }

    turn.result = result;
    turn.pending = false;
    deskState.busy = false;
    stopDeskTimer();
    if (needsTraceMatrix(result)) turn.matrixState = "loading";
    renderDeskThread(inst);
    updateDeskBusy(inst);
    RD.announceResult(result);
    if (activeDesk && activeDesk !== inst && activeDesk.alive) {
      renderDeskThread(activeDesk);
    }
    if (turn.matrixState === "loading") loadTraceMatrices(inst, turn);
  }

  let activeDesk = null;

  function renderDeskSource(inst) {
    if (!inst.source) return;
    if (!deskState.sourceDoc) {
      inst.source.hidden = true;
      inst.source.innerHTML = "";
      return;
    }
    const title = RD.documents.title(deskState.sourceDoc);
    inst.source.hidden = false;
    inst.source.innerHTML = '<span class="desk-source__label">Source note</span>' +
      '<a class="scope-chip" href="' + RD.escapeAttr(RD.docHref(deskState.sourceDoc)) + '">' +
        RD.esc(title) + "</a>" +
      '<span class="desk-source__note">The desk does not inherit the note’s scope silently. Focus stays as shown below.</span>' +
      '<button type="button" class="desk-source__remove" data-source-remove aria-label="Remove source note">×</button>';
  }

  function bindDesk(inst) {
    window.addEventListener("rd:coverage", inst.onCoverage);
    window.addEventListener("rd:documents", inst.onDocuments);

    inst.form.addEventListener("submit", (event) => {
      event.preventDefault();
      if (deskState.busy) return;
      const question = inst.input.value.trim();
      if (!question) {
        inst.form.classList.add("is-invalid");
        inst.input.focus();
        RD.announce("Type a question before filing.");
        return;
      }
      inst.form.classList.remove("is-invalid");
      askDesk(inst, question);
    });

    inst.input.addEventListener("input", () => {
      deskState.draft = inst.input.value;
      updateCharCount(inst);
      inst.form.classList.remove("is-invalid");
    });

    inst.input.addEventListener("keydown", (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key === "Enter") {
        event.preventDefault();
        if (!deskState.busy) inst.form.requestSubmit();
      }
    });

    inst.ticker.addEventListener("change", () => {
      deskState.ticker = inst.ticker.value;
      deskState.period = "";
      populateDeskPeriods(inst);
    });
    inst.period.addEventListener("change", () => {
      deskState.period = inst.period.value;
      updateDeskScopeNote(inst);
    });

    inst.coverageRetry.addEventListener("click", () => {
      if (typeof RD.reloadCoverage === "function") RD.reloadCoverage();
    });

    inst.prompts.addEventListener("click", (event) => {
      const button = event.target.closest("button[data-question]");
      if (!button) return;
      inst.input.value = button.dataset.question || "";
      deskState.draft = inst.input.value;
      updateCharCount(inst);
      inst.form.classList.remove("is-invalid");
      const ticker = button.dataset.ticker || "";
      if (ticker && companyFor(ticker)) {
        deskState.ticker = ticker;
        deskState.period = "";
        inst.ticker.value = ticker;
        populateDeskPeriods(inst);
      }
      inst.input.focus();
      RD.announce("Starter question loaded. Press Command or Control plus Enter to file.");
    });

    inst.clear.addEventListener("click", () => {
      if (deskState.busy) return;
      deskState.turns = [];
      deskState.seq = 0;
      renderDeskThread(inst);
      inst.input.focus();
      RD.announce("Desk cleared. No questions on file.");
    });

    inst.thread.addEventListener("click", (event) => {
      const retry = event.target.closest("[data-retry]");
      if (retry) {
        const article = retry.closest(".turn");
        const turn = article ? deskState.turns.find((item) => String(item.id) === article.dataset.turnId) : null;
        if (turn) askDesk(inst, turn.question, turn.scope);
        return;
      }

      const chip = event.target.closest(".cite-chip");
      if (chip) {
        const article = chip.closest(".turn");
        const turn = article ? deskState.turns.find((item) => String(item.id) === article.dataset.turnId) : null;
        if (!turn) return;
        const claimIndex = Number(chip.dataset.claim);
        const citeIndex = Number(chip.dataset.cite);
        const citation = RD.citations.from(turn.result, claimIndex, citeIndex);
        if (!citation) return;
        const documentId = RD.citations.document(citation);
        if (documentId) {
          RD.navigate(RD.docHref(documentId, {
            section: citation.section || citation.line || "",
            from: "desk",
            q: turn.question,
            quote: citation.quote || "",
          }));
          return;
        }
        const claim = RD.asArray(turn.result && turn.result.envelope && turn.result.envelope.claims)[claimIndex];
        RD.inspector.open({
          turn: turn,
          claim: claim,
          claimIndex: claimIndex,
          citation: citation,
          number: Number(chip.dataset.n),
          trigger: chip,
        });
      }
    });

    inst.root.addEventListener("click", (event) => {
      if (event.target.closest("[data-source-remove]")) {
        deskState.sourceDoc = "";
        renderDeskSource(inst);
      }
    });

    // Route hint: a question carried from the document workspace.
    const hinted = inst.route.params.get("q") || "";
    if (hinted && deskState.hintKey !== inst.hintKey) {
      deskState.hintKey = inst.hintKey;
      inst.input.value = hinted;
      deskState.draft = hinted;
      updateCharCount(inst);
      const hint = document.createElement("p");
      hint.className = "desk-hint";
      hint.textContent = inst.route.params.get("from") === "doc"
        ? "Question carried from the document workspace. Review the scope below, then file."
        : "Question loaded from the link. Review it, then file.";
      inst.form.parentElement.insertBefore(hint, inst.form);
    } else {
      inst.input.value = deskState.draft || "";
      updateCharCount(inst);
    }

    const hintTicker = (inst.route.params.get("ticker") || "").toUpperCase();
    if (inst.route.params.get("from") === "doc" && !hintTicker) {
      // Escalation from a note must not look like it inherited the note's scope.
      deskState.ticker = "";
      deskState.period = "";
    }
    if (hintTicker && companyFor(hintTicker)) {
      deskState.ticker = hintTicker;
      inst.ticker.value = hintTicker;
      const hintPeriod = inst.route.params.get("period") || "";
      const hintYear = inst.route.params.get("year") || "";
      const periodLabel = hintPeriod && hintYear ? hintPeriod + " " + hintYear : hintPeriod;
      if (periodLabel && RD.asArray(companyFor(hintTicker).periods).includes(periodLabel)) {
        deskState.period = periodLabel;
      }
      populateDeskPeriods(inst);
      inst.ticker.value = hintTicker;
      inst.period.value = deskState.period;
      updateDeskScopeNote(inst);
    } else {
      if (hintTicker) {
        // Coverage may still be in flight; apply the deep-link focus when it lands.
        deskState.pendingFocus = { ticker: hintTicker, period: inst.route.params.get("period") || "" };
      }
      populateDeskTickers(inst);
    }

    deskState.sourceDoc = inst.route.params.get("doc") || "";
    renderDeskSource(inst);
    renderDeskCoverage(inst);
    renderDeskThread(inst);
    updateDeskBusy(inst);
    updateDeskScopeNote(inst);
    updateCharCount(inst);
    if (deskState.turns.some((turn) => turn.pending)) startDeskTimer(inst);
  }

  const deskView = {
    mount(root, route) {
      root.innerHTML = deskShell();
      const inst = {
        root: root,
        route: route,
        hintKey: route.search || "",
        alive: true,
        thread: root.querySelector("#thread"),
        form: root.querySelector("#ask-form"),
        input: root.querySelector("#composer-input"),
        charCount: root.querySelector("#char-count"),
        askButton: root.querySelector("#ask-button"),
        askLabel: root.querySelector("#ask-button-label"),
        clear: root.querySelector("#clear-thread"),
        ticker: root.querySelector("#ticker-select"),
        period: root.querySelector("#period-select"),
        scopeNote: root.querySelector("#scope-note"),
        scopeLine: root.querySelector("#desk-scope-line"),
        statIssuers: root.querySelector('[data-stat="issuers"]'),
        statPeriods: root.querySelector('[data-stat="periods"]'),
        statOperational: root.querySelector('[data-stat="operational"]'),
        asOfText: root.querySelector("#desk-as-of-text"),
        asOf: root.querySelector("#desk-as-of"),
        editionDate: root.querySelector("#desk-edition-date"),
        coverageLine: root.querySelector("#desk-coverage-line"),
        coverageRetry: root.querySelector("#desk-coverage-retry"),
        prompts: root.querySelector("#desk-prompts"),
        source: root.querySelector("#desk-source"),
        onCoverage: () => {
          if (!inst.alive) return;
          renderDeskCoverage(inst);
          renderDeskThread(inst);
          renderDeskSource(inst);
        },
        onDocuments: () => {
          if (!inst.alive) return;
          renderDeskSource(inst);
          renderDeskThread(inst);
        },
      };
      deskState.instance = inst;
      activeDesk = inst;
      bindDesk(inst);
      RD.setTitle("Research desk · IDX Research Desk");
    },
    unmount() {
      const inst = deskState.instance;
      if (!inst) return;
      inst.alive = false;
      window.removeEventListener("rd:coverage", inst.onCoverage);
      window.removeEventListener("rd:documents", inst.onDocuments);
      stopDeskTimer();
      deskState.draft = inst.input ? inst.input.value : deskState.draft;
      deskState.pendingFocus = null;
      deskState.instance = null;
      activeDesk = null;
    },
  };

  RD.views.desk = deskView;
})(window);
