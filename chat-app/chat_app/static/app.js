/* IDX Research Desk — app shell and hash router.
 *
 * One static app, three routes:
 *   #/documents      the analysis vault
 *   #/doc/{id}       the reader + pinned document chat
 *   #/desk           the wide-scope research desk
 *
 * The hash is the source of truth. There is no server router; reload, back,
 * forward and copied links all restore the same surface. Views live in
 * document.js and desk.js and are registered on RD.views.
 */

"use strict";

(function (global) {
  const RD = global.RD;
  if (!RD) {
    document.body.innerHTML = '<p style="padding:2rem;font-family:serif">The client failed to load its shared module. Reload the page.</p>';
    return;
  }

  const STORAGE_THEME = "idx-desk-theme";
  const PRODUCT = "IDX Research Desk";

  const dom = {
    html: document.documentElement,
    app: document.getElementById("app"),
    topbar: document.getElementById("topbar"),
    coverageLine: document.getElementById("coverage-line"),
    feedDot: document.getElementById("feed-dot"),
    coverageRetry: document.getElementById("coverage-retry"),
    themeToggle: document.getElementById("theme-toggle"),
    themeLabel: document.getElementById("theme-label"),
    navLinks: Array.from(document.querySelectorAll("[data-nav]")),
    announcer: document.getElementById("announcer"),
  };

  const state = {
    route: null,
    view: null,
    mountedAt: 0,
  };

  /* ------------------------------------------------------------------------
     Routes
     ------------------------------------------------------------------------ */

  function safeDecode(value) {
    try {
      return decodeURIComponent(value);
    } catch (error) {
      return value;
    }
  }

  function parseRoute() {
    let raw = window.location.hash || "";
    if (raw.charAt(0) === "#") raw = raw.slice(1);
    if (!raw) raw = "/documents";

    const queryIndex = raw.indexOf("?");
    let path = queryIndex >= 0 ? raw.slice(0, queryIndex) : raw;
    const search = queryIndex >= 0 ? raw.slice(queryIndex) : "";
    const params = new URLSearchParams(queryIndex >= 0 ? raw.slice(queryIndex + 1) : "");

    path = path.replace(/\/+$/, "") || "/";
    if (!path.startsWith("/")) path = "/" + path;

    if (path === "/" || path === "/documents" || path === "/app") {
      return { name: "documents", path: "/documents", search: search, params: params };
    }
    if (path === "/desk") {
      return { name: "desk", path: "/desk", search: search, params: params };
    }

    const doc = /^\/doc\/([^/]+)$/.exec(path);
    if (doc) {
      return {
        name: "workspace",
        path: path,
        search: search,
        params: params,
        docId: safeDecode(doc[1]),
      };
    }

    return { name: "notfound", path: path, search: search, params: params };
  }

  function routeTitle(route) {
    if (route.name === "documents") return "Analysis notes · " + PRODUCT;
    if (route.name === "workspace") return "Document · " + PRODUCT;
    if (route.name === "desk") return "Research desk · " + PRODUCT;
    return "Route not found · " + PRODUCT;
  }

  function routeAnnouncement(route) {
    if (route.name === "documents") return "Analysis library opened.";
    if (route.name === "workspace") return "Document workspace opened.";
    if (route.name === "desk") return "Research desk opened.";
    return "That route does not exist.";
  }

  function updateNav(route) {
    dom.navLinks.forEach((link) => {
      const nav = link.getAttribute("data-nav");
      const active = (nav === "documents" && (route.name === "documents" || route.name === "workspace")) ||
        (nav === "desk" && route.name === "desk");
      if (active) link.setAttribute("aria-current", "page");
      else link.removeAttribute("aria-current");
    });
    document.body.dataset.surface = route.name;
  }

  /* ------------------------------------------------------------------------
     Typed error / not-found surfaces
     ------------------------------------------------------------------------ */

  function notFoundView() {
    return {
      mount(root) {
        root.innerHTML = '<div class="route-missing">' +
          '<p class="eyebrow"><span class="eyebrow__no">404</span>No such surface</p>' +
          '<h1 class="route-missing__title">This page is not in the ledger.</h1>' +
          '<p class="route-missing__body">The app has three surfaces. The address in the bar is not one of them. Use the top bar, or take one of these:</p>' +
          '<div class="panel__actions">' +
            '<a class="button-quiet" href="#/documents">Analysis library →</a>' +
            '<a class="button-quiet" href="#/desk">Research desk →</a>' +
          "</div>" +
          '<p class="route-missing__path">' + RD.esc(String(window.location.hash || "#/documents")) + "</p>" +
        "</div>";
      },
      unmount() {},
    };
  }

  function renderShellError(route, error) {
    const message = error && error.message ? error.message : String(error || "unknown error");
    dom.app.innerHTML = '<div class="route-missing">' +
      '<p class="eyebrow"><span class="eyebrow__no">ERR</span>Surface failed</p>' +
      '<h1 class="route-missing__title">This surface did not mount.</h1>' +
      '<p class="route-missing__body">The shell caught an error before the view could finish. Nothing partial is shown. Reload the surface, or move to the library.</p>' +
      '<p class="panel__error">' + RD.esc(message) + "</p>" +
      '<div class="panel__actions">' +
        '<button type="button" class="button-quiet" data-route-retry>Retry surface</button>' +
        '<a class="button-quiet" href="#/documents">← Library</a>' +
        '<a class="button-quiet" href="#/desk">Desk</a>' +
      "</div>" +
    "</div>";
    dom.app.querySelector("[data-route-retry]").addEventListener("click", () => renderRoute(true));
    RD.setTitle("Surface error · " + PRODUCT);
    RD.state.route = route;
  }

  /* ------------------------------------------------------------------------
     Router
     ------------------------------------------------------------------------ */

  function renderRoute(force) {
    const route = parseRoute();
    const same = state.route && state.route.path === route.path && state.route.search === route.search;
    if (same && !force) return;

    if (state.view && typeof state.view.unmount === "function") {
      try {
        state.view.unmount();
      } catch (error) {
        /* a view must not be able to block navigation */
      }
    }
    RD.inspector.close();
    state.route = route;
    state.view = null;
    RD.state.route = route;
    updateNav(route);
    dom.app.innerHTML = "";

    const view = RD.views[route.name] || notFoundView();
    state.view = view;
    state.mountedAt = Date.now();

    document.title = routeTitle(route);
    try {
      view.mount(dom.app, route);
    } catch (error) {
      renderShellError(route, error);
      return;
    }

    try {
      window.scrollTo({ top: 0, behavior: "auto" });
    } catch (error) {
      window.scrollTo(0, 0);
    }
    if (dom.app) {
      try {
        dom.app.focus({ preventScroll: true });
      } catch (error) {
        dom.app.focus();
      }
    }
    RD.announce(routeAnnouncement(route));
  }

  function rerender() {
    renderRoute(true);
  }

  RD.setTitle = function setTitle(value) {
    document.title = String(value || "").trim() || PRODUCT;
  };
  RD.rerender = rerender;
  RD.getRoute = () => state.route;

  /* ------------------------------------------------------------------------
     Theme
     ------------------------------------------------------------------------ */

  function currentTheme() {
    return dom.html.getAttribute("data-theme") === "dark" ? "dark" : "light";
  }

  function syncThemeLabel() {
    const dark = currentTheme() === "dark";
    if (dom.themeToggle) dom.themeToggle.setAttribute("aria-pressed", String(dark));
    if (dom.themeLabel) dom.themeLabel.textContent = dark ? "Lights up" : "Lights down";
  }

  function bindTheme() {
    syncThemeLabel();
    if (!dom.themeToggle) return;
    dom.themeToggle.addEventListener("click", () => {
      const next = currentTheme() === "dark" ? "light" : "dark";
      dom.html.setAttribute("data-theme", next);
      try {
        localStorage.setItem(STORAGE_THEME, next);
      } catch (error) {
        /* private mode: the theme simply does not persist */
      }
      syncThemeLabel();
      RD.announce(next === "dark" ? "Ink theme on." : "Paper theme on.");
    });
  }

  /* ------------------------------------------------------------------------
     Coverage feed (GET /api/companies)
     ------------------------------------------------------------------------ */

  function setTopbarCoverage(text, dotState, showRetry) {
    if (dom.coverageLine) dom.coverageLine.textContent = text;
    if (dom.coverageLine && dom.coverageLine.parentElement) {
      dom.coverageLine.parentElement.setAttribute("aria-label", "Coverage: " + text);
    }
    if (dom.feedDot) dom.feedDot.dataset.state = dotState;
    if (dom.coverageRetry) dom.coverageRetry.hidden = !showRetry;
  }

  async function loadCoverage() {
    RD.state.coverageState = "loading";
    RD.state.coverageError = null;
    setTopbarCoverage("Wiring the coverage feed…", "loading", false);
    window.dispatchEvent(new CustomEvent("rd:coverage", { detail: RD.state }));

    try {
      const response = await fetch("/api/companies", {
        headers: { Accept: "application/json" },
        cache: "no-store",
      });
      const payload = await RD.readJson(response);
      if (!response.ok) {
        throw new Error(RD.errorMessage(payload, "Coverage feed answered " + response.status + "."));
      }
      if (!payload || payload.ok === false) {
        throw new Error(RD.errorMessage(payload, "Coverage feed returned an error."));
      }
      const companies = RD.asArray(payload.companies).filter((company) => company && company.ticker);
      if (!companies.length) throw new Error("Coverage feed returned no issuers.");
      RD.coverage.setCompanies(companies, payload.as_of);
      RD.state.coverageState = "ready";
      RD.state.coverageError = null;
      const stats = RD.coverage.stats();
      setTopbarCoverage(
        stats.issuers + " issuers · " + stats.periods + " periods · " +
          stats.operational + " op. " + (stats.operational === 1 ? "set" : "sets") +
          " · not IDX-wide · as of " + RD.formatDate(stats.asOf),
        "ready",
        false
      );
    } catch (error) {
      RD.coverage.setCompanies([], null);
      RD.state.coverageState = "error";
      RD.state.coverageError = error;
      setTopbarCoverage("Coverage unavailable — the desk will not imply a supported set.", "error", true);
    }
    window.dispatchEvent(new CustomEvent("rd:coverage", { detail: RD.state }));
  }

  RD.reloadCoverage = loadCoverage;

  /* ------------------------------------------------------------------------
     Boot
     ------------------------------------------------------------------------ */

  function bindChrome() {
    bindTheme();
    const skip = document.querySelector(".skip-link");
    if (skip) {
      skip.addEventListener("click", (event) => {
        event.preventDefault();
        if (dom.app) dom.app.focus();
        RD.announce("Surface focused.");
      });
    }
    if (dom.coverageRetry) dom.coverageRetry.addEventListener("click", loadCoverage);
    document.getElementById("app").addEventListener("click", (event) => {
      const retry = event.target.closest("[data-route-retry]");
      if (retry) renderRoute(true);
    });
  }

  function boot() {
    bindChrome();

    // Normalise an empty hash without adding a history entry.
    if (!window.location.hash) {
      try {
        history.replaceState(null, "", "#/documents");
      } catch (error) {
        window.location.hash = "#/documents";
      }
    }

    window.addEventListener("hashchange", () => renderRoute(false));
    renderRoute(true);
    loadCoverage();

    // The document index powers wiki links and desk → note links. Fetch it
    // quietly; the library reuses the same in-flight promise.
    RD.documents.load().catch(() => {
      /* the library renders the designed offline state when opened */
    });
  }

  boot();
})(window);
