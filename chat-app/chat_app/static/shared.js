/* IDX Research Desk — shared client primitives.
 *
 * Loaded before app.js and the two surface modules. Everything that renders
 * document or model text lives here: an escape-first markdown renderer (pipe
 * tables, fenced code, blockquotes, safe links/images, Obsidian wiki links),
 * the citation chip vocabulary, the answer envelope renderer and the evidence
 * inspector.
 *
 * Rules kept deliberately structural:
 *   - no raw HTML from a document or the model ever reaches innerHTML;
 *   - inline text is escaped as it is emitted, not after markup is parsed;
 *   - links and images pass an http(s)/relative protocol allowlist;
 *   - images are lazy and carry referrerpolicy="no-referrer".
 */

"use strict";

(function (global) {
  const RD = (global.RD = global.RD || {});

  const MAX_HISTORY_MESSAGES = 8;
  const CLIENT_TIMEOUT_MS = 200000;
  const MAX_QUOTE_LENGTH = 600;

  RD.views = RD.views || {};
  RD.MAX_HISTORY_MESSAGES = MAX_HISTORY_MESSAGES;
  RD.CLIENT_TIMEOUT_MS = CLIENT_TIMEOUT_MS;

  RD.state = RD.state || {
    companies: [],
    asOf: null,
    coverageState: "loading",
    coverageError: null,
    route: null,
  };

  /* ------------------------------------------------------------------------
     Small utilities
     ------------------------------------------------------------------------ */

  function asArray(value) {
    if (Array.isArray(value)) return value;
    if (value === undefined || value === null) return [];
    return [value];
  }

  function objectOr(value) {
    return value && typeof value === "object" && !Array.isArray(value) ? value : {};
  }

  function escapeHtml(value) {
    return String(value == null ? "" : value).replace(/[&<>"']/g, (char) => ({
      "&": "&amp;",
      "<": "&lt;",
      ">": "&gt;",
      '"': "&quot;",
      "'": "&#39;",
    }[char]));
  }

  function escapeAttr(value) {
    return escapeHtml(value).replace(/`/g, "&#96;");
  }

  function pad2(value) {
    return String(value).padStart(2, "0");
  }

  function debounce(fn, wait) {
    let timer = null;
    return function debounced() {
      const args = arguments;
      window.clearTimeout(timer);
      timer = window.setTimeout(() => fn.apply(this, args), wait);
    };
  }

  function prefersReducedMotion() {
    try {
      return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch (error) {
      return false;
    }
  }

  function isMobile() {
    try {
      return window.matchMedia("(max-width: 959px)").matches;
    } catch (error) {
      return false;
    }
  }

  const numberFormat = new Intl.NumberFormat("en-US", {
    maximumFractionDigits: 2,
  });

  function formatNumber(value) {
    if (typeof value !== "number" || !Number.isFinite(value)) {
      return String(value === undefined || value === null ? "—" : value);
    }
    return numberFormat.format(value);
  }

  function formatCost(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return "—";
    if (number === 0) return "$0.0000";
    if (number < 0.01) return "$" + number.toFixed(4);
    return "$" + number.toFixed(3);
  }

  function formatDuration(value) {
    const number = Number(value);
    if (!Number.isFinite(number)) return "—";
    return number.toFixed(1) + " s";
  }

  function formatDate(value) {
    if (!value) return "—";
    const text = String(value);
    const date = new Date(text.length > 10 ? text : text.slice(0, 10) + "T00:00:00");
    if (Number.isNaN(date.getTime())) return text;
    try {
      return new Intl.DateTimeFormat("en-GB", {
        day: "numeric",
        month: "short",
        year: "numeric",
      }).format(date);
    } catch (error) {
      return text;
    }
  }

  function formatClock(value) {
    const date = value ? new Date(value) : new Date();
    if (Number.isNaN(date.getTime())) return "";
    try {
      return new Intl.DateTimeFormat("en-GB", {
        hour: "2-digit",
        minute: "2-digit",
      }).format(date);
    } catch (error) {
      return "";
    }
  }

  function formatBytes(value) {
    const number = Number(value);
    if (!Number.isFinite(number) || number <= 0) return "—";
    if (number < 1024) return number + " B";
    if (number < 1024 * 1024) return (number / 1024).toFixed(1) + " KB";
    return (number / (1024 * 1024)).toFixed(1) + " MB";
  }

  function humanState(value) {
    return String(value || "unknown").replace(/_/g, " ");
  }

  function humanKey(key) {
    const labels = {
      url: "Source URL",
      http_status: "HTTP status",
      retrieved_at: "Retrieved",
      period_end: "Period end",
      days_after_period_end: "Days after period end",
      latest_captured_release: "Latest captured release",
      latest_captured_document: "Latest document",
      document_id: "Document",
      page_range: "Pages",
      page: "Page",
      expected_documents: "Expected documents",
      supported_periods: "Supported periods",
      supported_tickers: "Supported tickers",
      as_of: "As of",
      detail: "Detail",
      error: "Error",
      where: "Where",
      item: "Item",
      state: "State",
      section: "Section",
      line: "Line",
      quote: "Quote",
      ticker: "Ticker",
      period: "Period",
      year: "Year",
      kind: "Kind",
      updated_at: "Updated",
      path: "Path",
    };
    return labels[key] || String(key).replace(/_/g, " ");
  }

  function announce(message) {
    const node = document.getElementById("announcer");
    if (!node) return;
    node.textContent = "";
    window.setTimeout(() => {
      node.textContent = String(message || "");
    }, 40);
  }

  async function readJson(response) {
    const text = await response.text();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch (error) {
      return { error: text.slice(0, 500) };
    }
  }

  function errorMessage(payload, fallback) {
    if (!payload) return fallback;
    if (typeof payload.error === "string" && payload.error.trim()) return payload.error;
    if (payload.detail) {
      if (typeof payload.detail === "string") return payload.detail;
      if (Array.isArray(payload.detail)) {
        return payload.detail
          .map((entry) => entry && (entry.msg || entry.type) || "")
          .filter(Boolean)
          .join("; ") || fallback;
      }
      try {
        return JSON.stringify(payload.detail);
      } catch (error) {
        return fallback;
      }
    }
    return fallback;
  }

  function safeUrl(raw) {
    if (raw === undefined || raw === null) return null;
    let value = String(raw).trim();
    if (!value) return null;
    // Strip control characters and any whitespace a parser might ignore.
    value = value.replace(/[\u0000-\u0020\u007f]+/g, "");
    if (value.includes("\\")) return null;
    if (/^https?:\/\//i.test(value)) return value;
    if (/^\/\//.test(value)) return null; // protocol-relative: external, disallowed
    if (/^\/(?!\/)/.test(value)) return value;
    if (/^\.{1,2}\//.test(value)) return value;
    if (/^#/.test(value)) return value;
    if (/^\?/.test(value)) return value;
    if (!/^[a-z][a-z0-9+.-]*:/i.test(value)) return value; // plain relative path
    return null;
  }

  function slugify(value) {
    const text = String(value === undefined || value === null ? "" : value)
      .normalize("NFKD")
      .replace(/[\u0300-\u036f]/g, "")
      .replace(/\[\[([^\]|]+)\|([^\]]+)\]\]/g, "$2")
      .replace(/\[\[([^\]]+)\]\]/g, "$1")
      .replace(/[*_`]/g, "")
      .replace(/[^\w\s-]/g, " ")
      .trim()
      .toLowerCase();
    return text.replace(/[\s_]+/g, "-").replace(/-+/g, "-").replace(/^-|-$/g, "") || "section";
  }

  /* ------------------------------------------------------------------------
     Markdown — escape-first
     ------------------------------------------------------------------------ */

  function renderInline(raw, options, depth) {
    const o = options || {};
    const level = depth || 0;
    if (level > 4) return escapeHtml(raw);

    let output = "";
    let buffer = "";
    let index = 0;

    function flush() {
      if (!buffer) return;
      output += escapeHtml(buffer);
      buffer = "";
    }

    while (index < raw.length) {
      const char = raw[index];

      if (char === "`") {
        let run = 1;
        while (raw[index + run] === "`") run += 1;
        const marker = "`".repeat(run);
        const close = raw.indexOf(marker, index + run);
        if (close !== -1) {
          flush();
          let code = raw.slice(index + run, close).replace(/\n/g, " ");
          if (/^ .* $/.test(code) && code.trim()) code = code.slice(1, -1);
          output += "<code>" + escapeHtml(code) + "</code>";
          index = close + run;
          continue;
        }
      }

      if (char === "!" && raw[index + 1] === "[") {
        const link = matchInlineLink(raw, index + 1);
        if (link) {
          flush();
          const source = o.images !== false ? safeUrl(link.url) : null;
          if (source) {
            output += '<img src="' + escapeAttr(source) + '" alt="' + escapeAttr(link.text) + '"' +
              (link.title ? ' title="' + escapeAttr(link.title) + '"' : "") +
              ' loading="lazy" decoding="async" referrerpolicy="no-referrer">';
          } else {
            output += escapeHtml(link.text);
          }
          index = link.end;
          continue;
        }
      }

      if (char === "[") {
        if (raw[index + 1] === "[") {
          const close = raw.indexOf("]]", index + 2);
          if (close !== -1) {
            const inner = raw.slice(index + 2, close);
            const pieces = inner.split("|");
            const target = (pieces[0] || "").trim();
            const label = (pieces.length > 1 ? pieces.slice(1).join("|") : pieces[0] || "").trim();
            if (target) {
              flush();
              const resolved = o.resolveWiki === false ? null : documents.resolve(target);
              const text = renderInline(label || target, o, level + 1);
              if (resolved) {
                output += '<a class="wiki-link" href="' + escapeAttr(docHref(resolved.id, {
                  section: resolved.section || "",
                })) + '">' + text + "</a>";
              } else {
                output += text;
              }
              index = close + 2;
              continue;
            }
          }
        } else {
          const link = matchInlineLink(raw, index);
          if (link) {
            flush();
            const label = renderInline(link.text, o, level + 1);
            const href = safeUrl(link.url);
            if (href && o.links !== false) {
              const external = /^https?:\/\//i.test(href);
              output += '<a href="' + escapeAttr(href) + '"' +
                (external ? ' target="_blank" rel="noopener noreferrer"' : "") +
                (link.title ? ' title="' + escapeAttr(link.title) + '"' : "") +
                (href.charAt(0) === "#" ? ' data-internal-link="true"' : "") +
                ">" + label + "</a>";
            } else {
              output += label;
            }
            index = link.end;
            continue;
          }
        }
      }

      if (char === "*") {
        const strong = raw[index + 1] === "*";
        const marker = strong ? "**" : "*";
        const close = raw.indexOf(marker, index + marker.length);
        if (close > index + marker.length) {
          const body = raw.slice(index + marker.length, close);
          const startsSpace = /^\s/.test(body);
          const endsSpace = /\s$/.test(body);
          if (body && !startsSpace && !endsSpace) {
            flush();
            const tag = strong ? "strong" : "em";
            output += "<" + tag + ">" + renderInline(body, o, level + 1) + "</" + tag + ">";
            index = close + marker.length;
            continue;
          }
        }
      }

      buffer += char;
      index += 1;
    }

    flush();
    return output;
  }

  function matchInlineLink(raw, start) {
    if (raw[start] !== "[") return null;
    let index = start + 1;
    let depth = 0;
    let textEnd = -1;
    for (; index < raw.length; index += 1) {
      const char = raw[index];
      if (char === "\\") {
        index += 1;
        continue;
      }
      if (char === "\n") return null;
      if (char === "[") depth += 1;
      else if (char === "]") {
        if (depth === 0) {
          textEnd = index;
          break;
        }
        depth -= 1;
      }
    }
    if (textEnd < 0 || raw[textEnd + 1] !== "(") return null;

    let cursor = textEnd + 2;
    let parens = 0;
    let close = -1;
    for (; cursor < raw.length; cursor += 1) {
      const char = raw[cursor];
      if (char === "\\") {
        cursor += 1;
        continue;
      }
      if (char === "\n") return null;
      if (char === "(") parens += 1;
      else if (char === ")") {
        if (parens === 0) {
          close = cursor;
          break;
        }
        parens -= 1;
      }
    }
    if (close < 0) return null;

    const text = raw.slice(start + 1, textEnd);
    let inner = raw.slice(textEnd + 2, close).trim();
    let url = inner;
    let title = "";
    if (inner.charAt(0) === "<") {
      const gt = inner.indexOf(">");
      if (gt > 0) {
        url = inner.slice(1, gt);
        const rest = inner.slice(gt + 1).trim();
        const titleMatch = /^["'](.*)["']$/.exec(rest);
        if (titleMatch) title = titleMatch[1];
      }
    } else {
      const match = /^(\S+)\s+["'](.*)["']$/.exec(inner);
      if (match) {
        url = match[1];
        title = match[2];
      }
    }
    return { text: text, url: url, title: title, end: close + 1 };
  }

  function splitTableRow(line) {
    let text = line.trim();
    if (text.startsWith("|")) text = text.slice(1);
    if (text.endsWith("|") && !text.endsWith("\\|")) text = text.slice(0, -1);
    const cells = [];
    let current = "";
    for (let i = 0; i < text.length; i += 1) {
      const char = text[i];
      if (char === "\\" && text[i + 1] === "|") {
        current += "|";
        i += 1;
        continue;
      }
      if (char === "|") {
        cells.push(current.trim());
        current = "";
        continue;
      }
      current += char;
    }
    cells.push(current.trim());
    return cells;
  }

  function tableDelimiter(line) {
    const clean = line.trim();
    if (!clean.includes("-")) return false;
    const cells = splitTableRow(clean);
    if (!cells.length) return false;
    return cells.every((cell) => /^:?-{2,}:?$/.test(cell.replace(/\s+/g, "")));
  }

  function alignmentFor(line) {
    return splitTableRow(line).map((cell) => {
      const value = cell.replace(/\s+/g, "");
      const left = value.startsWith(":");
      const right = value.endsWith(":");
      if (left && right) return "center";
      if (right) return "right";
      return "left";
    });
  }

  function isFence(line) {
    return /^\s*(```+|~~~+)\s*([^`]*)$/.exec(line);
  }

  function isListline(line) {
    return /^(\s*)([-*+]|\d+[.)])\s+(.*)$/.test(line);
  }

  function isBlockStart(line) {
    const trimmed = line.trim();
    return !trimmed ||
      /^#{1,6}\s+/.test(trimmed) ||
      /^```|^~~~/.test(trimmed) ||
      /^>/.test(trimmed) ||
      /^(-{3,}|\*{3,}|_{3,})$/.test(trimmed) ||
      isListline(line);
  }

  function renderBlocks(lines, options, startLine) {
    const o = options || {};
    const out = [];
    const headings = [];
    let index = 0;
    let sectionOpen = false;
    let usedIds = {};
    const baseLine = startLine || 1;

    function documentMode() {
      return Boolean(o.document);
    }

    function ensureSection(line) {
      if (!documentMode() || sectionOpen) return;
      const id = "front-matter-" + baseLine;
      const anchor = usedIds[id] ? id + "-2" : id;
      usedIds[anchor] = true;
      out.push('<section class="doc-section doc-section--front" data-section="' + escapeAttr(anchor) +
        '" data-line="' + line + '">');
      sectionOpen = true;
    }

    while (index < lines.length) {
      const line = lines[index];
      const lineNo = baseLine + index;
      const trimmed = line.trim();

      if (!trimmed) {
        index += 1;
        continue;
      }

      const fence = isFence(line);
      if (fence) {
        ensureSection(lineNo);
        const marker = fence[1].slice(0, 3);
        const language = (fence[2] || "").trim().split(/\s+/)[0] || "";
        const body = [];
        index += 1;
        while (index < lines.length && !lines[index].trim().startsWith(marker)) {
          body.push(lines[index]);
          index += 1;
        }
        index += 1; // closing fence
        out.push('<pre class="code-block" data-line="' + lineNo + '">' +
          (language ? '<span class="code-block__lang">' + escapeHtml(language) + "</span>" : "") +
          "<code>" + escapeHtml(body.join("\n")) + "</code></pre>");
        continue;
      }

      const heading = /^(#{1,6})\s+(.*)$/.exec(trimmed);
      if (heading) {
        const level = heading[1].length;
        const title = heading[2].replace(/\s+#+\s*$/, "").trim();
        let id = o.sectionIds && o.sectionIds[lineNo];
        if (!id) id = slugify(title);
        let unique = id;
        let suffix = 2;
        while (usedIds[unique]) {
          unique = id + "-" + suffix;
          suffix += 1;
        }
        usedIds[unique] = true;
        headings.push({ id: unique, title: title, level: level, line: lineNo });

        if (documentMode()) {
          if (sectionOpen) out.push("</section>");
          out.push('<section class="doc-section" data-section="' + escapeAttr(unique) +
            '" data-line="' + lineNo + '" data-level="' + level + '">');
          sectionOpen = true;
        }

        const tag = "h" + Math.min(level, 6);
        const anchorLabel = escapeAttr(title || unique);
        const anchorButton = documentMode()
          ? '<button type="button" class="heading-anchor" data-anchor="' + escapeAttr(unique) +
            '" data-anchor-title="' + anchorLabel + '" aria-label="Copy link to section: ' +
            anchorLabel + '">#</button>'
          : "";
        out.push("<" + tag + (documentMode() ? ' id="' + escapeAttr(unique) + '"' : "") +
          ' class="doc-h doc-h--' + level +
          '" data-line="' + lineNo + '">' +
          renderInline(title, o, 1) + anchorButton +
          "</" + tag + ">");
        index += 1;
        continue;
      }

      if (/^(-{3,}|\*{3,}|_{3,})$/.test(trimmed)) {
        ensureSection(lineNo);
        out.push('<hr class="prose-hr" data-line="' + lineNo + '">');
        index += 1;
        continue;
      }

      if (/^>/.test(trimmed)) {
        ensureSection(lineNo);
        const quoteLines = [];
        while (index < lines.length && /^\s*>/.test(lines[index])) {
          quoteLines.push(lines[index].replace(/^\s*>\s?/, ""));
          index += 1;
        }
        const inner = renderBlocks(quoteLines, Object.assign({}, o, {
          document: false,
          sectionIds: null,
        }), lineNo);
        out.push('<blockquote class="doc-quote" data-line="' + lineNo + '">' + inner.html + "</blockquote>");
        continue;
      }

      if (trimmed.includes("|") && index + 1 < lines.length && tableDelimiter(lines[index + 1])) {
        ensureSection(lineNo);
        const header = splitTableRow(trimmed);
        const align = alignmentFor(lines[index + 1]);
        index += 2;
        const rows = [];
        while (index < lines.length && lines[index].trim() && lines[index].includes("|")) {
          rows.push(splitTableRow(lines[index]));
          index += 1;
        }
        const head = header.map((cell, cellIndex) =>
          '<th scope="col" class="align-' + (align[cellIndex] || "left") + '">' +
          renderInline(cell, o, 1) + "</th>"
        ).join("");
        const body = rows.map((row) => "<tr>" + header.map((cell, cellIndex) =>
          '<td class="align-' + (align[cellIndex] || "left") + '">' +
          renderInline(row[cellIndex] === undefined ? "" : row[cellIndex], o, 1) + "</td>"
        ).join("") + "</tr>").join("");
        out.push('<div class="table-wrap" data-line="' + lineNo + '"><table class="doc-table">' +
          "<thead><tr>" + head + "</tr></thead><tbody>" + body + "</tbody></table></div>");
        continue;
      }

      if (isListline(line)) {
        ensureSection(lineNo);
        const items = [];
        while (index < lines.length) {
          const current = lines[index];
          const match = /^(\s*)([-*+]|\d+[.)])\s+(.*)$/.exec(current);
          if (match) {
            const indent = match[1].replace(/\t/g, "    ").length;
            const ordered = /\d/.test(match[2]);
            const text = match[3];
            const task = /^\[([ xX])\]\s+(.*)$/.exec(text);
            items.push({
              indent: indent,
              ordered: ordered,
              text: task ? task[2] : text,
              task: task ? task[1].toLowerCase() === "x" : null,
              line: lineNo + (items.length ? 0 : 0),
            });
            index += 1;
            continue;
          }
          if (current.trim() && /^\s{2,}\S/.test(current) && items.length) {
            items[items.length - 1].text += " " + current.trim();
            index += 1;
            continue;
          }
          break;
        }
        const listHtml = renderList(items, o, lineNo);
        out.push('<div class="list-wrap" data-line="' + lineNo + '">' + listHtml + "</div>");
        continue;
      }

      // Paragraph
      ensureSection(lineNo);
      const paragraph = [trimmed];
      index += 1;
      while (index < lines.length && lines[index].trim() && !isBlockStart(lines[index])) {
        paragraph.push(lines[index].trim());
        index += 1;
      }
      const paragraphHtml = paragraph.map((piece) =>
        renderInline(piece.replace(/\\$/, ""), o, 1)
      ).join("<br>");
      out.push('<p class="doc-p" data-line="' + lineNo + '">' + paragraphHtml + "</p>");
    }

    if (sectionOpen) out.push("</section>");
    return { html: out.join(""), headings: headings };
  }

  function renderList(items, options, line) {
    let cursor = 0;

    function build(indent) {
      if (cursor >= items.length) return "";
      const ordered = items[cursor].ordered;
      const tag = ordered ? "ol" : "ul";
      let html = "<" + tag + ' class="doc-list" data-line="' + (line + cursor) + '">';
      while (cursor < items.length) {
        const item = items[cursor];
        if (item.indent < indent) break;
        if (item.indent > indent) {
          // Nested list belongs to the previous li; append it before closing.
          const nested = build(item.indent);
          html = html.replace(/<\/li>$/, nested + "</li>");
          continue;
        }
        cursor += 1;
        let inner = "";
        if (item.task !== null) {
          inner += '<span class="task-box" aria-hidden="true">' + (item.task ? "✓" : "") + "</span>";
        }
        inner += renderInline(item.text, options, 1);
        // Peek: a deeper item opens a nested list inside this li.
        let close = true;
        if (cursor < items.length && items[cursor].indent > indent) {
          close = false;
          html += "<li>" + inner;
        } else {
          html += "<li>" + inner + "</li>";
        }
        if (!close) {
          const nested = build(items[cursor].indent);
          html += nested + "</li>";
        }
      }
      return html + "</" + tag + ">";
    }

    return build(items.length ? items[0].indent : 0);
  }

  function renderMarkdown(source, options) {
    const raw = String(source === undefined || source === null ? "" : source)
      .replace(/\r\n/g, "\n")
      .replace(/\r/g, "\n");
    if (!raw.trim()) return '<p class="prose-empty">No prose on file.</p>';
    return renderBlocks(raw.split("\n"), Object.assign({}, options, { document: false }), 1).html;
  }

  function renderDocument(source, options) {
    const o = options || {};
    const raw = String(source === undefined || source === null ? "" : source)
      .replace(/\r\n/g, "\n")
      .replace(/\r/g, "\n");
    if (!raw.trim()) return { html: '<p class="prose-empty">This note is empty.</p>', headings: [] };
    return renderBlocks(
      raw.split("\n"),
      Object.assign({}, o, { document: true }),
      Number(o.startLine) > 0 ? Number(o.startLine) : 1
    );
  }

  /* ------------------------------------------------------------------------
     Frontmatter (metadata, never prose)
     ------------------------------------------------------------------------ */

  function parseScalar(raw) {
    let value = String(raw === undefined || raw === null ? "" : raw).trim();
    if (!value) return "";
    if (/^".*"$/.test(value) || /^'.*'$/.test(value)) {
      return value.slice(1, -1).replace(/\\(["'\\])/g, "$1");
    }
    if (/^\[.*\]$/.test(value)) {
      const inner = value.slice(1, -1).trim();
      if (!inner) return [];
      return inner.split(",").map((part) => parseScalar(part));
    }
    if (value === "true") return true;
    if (value === "false") return false;
    if (value === "null" || value === "~") return null;
    if (/^-?\d+(\.\d+)?$/.test(value)) return Number(value);
    return value.replace(/\s+#.*$/, "");
  }

  function parseFrontmatter(source) {
    const text = String(source === undefined || source === null ? "" : source)
      .replace(/\r\n/g, "\n")
      .replace(/^\uFEFF/, "");
    const lines = text.split("\n");
    if (!lines.length || lines[0].trim() !== "---") {
      return { meta: {}, body: text, has: false };
    }
    let close = -1;
    for (let index = 1; index < lines.length; index += 1) {
      if (lines[index].trim() === "---") {
        close = index;
        break;
      }
    }
    if (close < 0) {
      // An unterminated frontmatter block is metadata-like, not prose. Never
      // render the opening delimiter as a horizontal rule.
      return { meta: {}, body: lines.slice(1).join("\n"), has: false, bodyStart: 2, unclosed: true };
    }

    const meta = {};
    let currentKey = null;
    for (let index = 1; index < close; index += 1) {
      const line = lines[index];
      if (!line.trim() || /^\s*#/.test(line)) continue;
      const item = /^\s*-\s+(.*)$/.exec(line);
      if (item && currentKey) {
        if (!Array.isArray(meta[currentKey])) meta[currentKey] = [];
        meta[currentKey].push(parseScalar(item[1]));
        continue;
      }
      const pair = /^([A-Za-z0-9_.-]+):\s*(.*)$/.exec(line);
      if (pair) {
        currentKey = pair[1];
        meta[currentKey] = parseScalar(pair[2]);
        continue;
      }
      if (currentKey && /^\s+/.test(line)) {
        // Nested YAML: keep it readable rather than trying to be a parser.
        const nested = line.trim();
        meta[currentKey] = String(meta[currentKey] || "") + (meta[currentKey] ? " · " : "") + nested;
      }
    }

    const rawBodyLines = lines.slice(close + 1);
    let leading = 0;
    while (leading < rawBodyLines.length && !rawBodyLines[leading].trim()) leading += 1;
    const body = rawBodyLines.slice(leading).join("\n");
    return { meta: meta, body: body, has: true, bodyStart: close + 2 + leading };
  }

  /* ------------------------------------------------------------------------
     Citation vocabulary
     ------------------------------------------------------------------------ */

  function normalizeClaimType(type) {
    const value = String(type || "").toLowerCase();
    if (value === "fact") return "fact";
    if (value === "management_stated" || value === "management" || value === "mgmt") {
      return "management_stated";
    }
    if (value === "analysis_note" || value === "note" || value === "analysis") {
      return "analysis_note";
    }
    if (value === "hypothesis" || value === "hyp") return "hypothesis";
    return "hypothesis";
  }

  function badgeLabel(type, full) {
    const normalized = normalizeClaimType(type);
    if (full) {
      return {
        fact: "FACT",
        management_stated: "MANAGEMENT STATED",
        analysis_note: "ANALYSIS NOTE",
        hypothesis: "HYPOTHESIS",
      }[normalized];
    }
    return {
      fact: "FACT",
      management_stated: "MGMT",
      analysis_note: "NOTE",
      hypothesis: "HYP",
    }[normalized];
  }

  function badgeDescription(type) {
    const normalized = normalizeClaimType(type);
    return {
      fact: "Reported by a filing or a typed tool value, or arithmetic over tool values.",
      management_stated: "The company said it, quoted from a captured document.",
      analysis_note: "Reproduced from an analyst note; secondary, not XBRL-audited.",
      hypothesis: "The model's interpretation, not a reported fact.",
    }[normalized];
  }

  function violationLabel(type) {
    const labels = {
      number_not_in_ledger: "Number not in ledger",
      causal_claim_unlabelled: "Causal claim unlabelled",
      management_stated_without_document: "No document for management claim",
      analysis_note_without_citation: "Note claim without section citation",
      missing_without_documents_status: "Missing state without evidence",
      citation_unmatched: "Citation unmatched",
      ranking_language: "Ranking or advice language",
      unsupported_claim: "Unsupported claim",
    };
    return labels[type] || humanState(type);
  }

  function citationDocument(citation) {
    const item = citation || {};
    return item.document || item.document_id || "";
  }

  function citationSection(citation) {
    const item = citation || {};
    return item.section || item.section_id || "";
  }

  function isDocumentCitation(citation) {
    const item = citation || {};
    return Boolean(citationDocument(item)) ||
      item.tool === "analysis_document" ||
      item.tool === "analysis_search" ||
      item.tool === "list_documents";
  }

  function citationSummary(citation) {
    const item = citation || {};
    const parts = [item.tool || "citation"];
    if (item.ticker) parts.push(item.ticker);
    if (item.metric) parts.push(item.metric);
    if (item.driver) parts.push(item.driver);
    if (item.doc) parts.push(item.doc);
    if (item.document || item.document_id) parts.push(item.document || item.document_id);
    if (item.section) parts.push(item.section);
    if (item.line !== undefined && item.line !== null) parts.push("line " + item.line);
    if (item.page !== undefined && item.page !== null) parts.push("p." + item.page);
    if (item.row !== undefined && item.row !== null) parts.push("row " + item.row);
    return parts.join(" · ");
  }

  function citationText(active) {
    const citation = (active && active.citation) || {};
    const parts = ["Citation " + (active && active.number !== undefined ? active.number : "—")];
    if (citation.tool) parts.push(citation.tool);
    if (citation.ticker) parts.push(citation.ticker);
    if (citation.metric) parts.push("metric: " + citation.metric);
    if (citation.driver) parts.push("driver: " + citation.driver);
    if (citation.doc) parts.push("doc: " + citation.doc);
    if (citation.document || citation.document_id) parts.push("document: " + (citation.document || citation.document_id));
    if (citation.section) parts.push("section: " + citation.section);
    if (citation.line !== undefined && citation.line !== null) parts.push("line " + citation.line);
    if (citation.page !== undefined && citation.page !== null) parts.push("page " + citation.page);
    if (citation.row !== undefined && citation.row !== null) parts.push("row " + citation.row);
    if (citation.quote) parts.push('quote: "' + citation.quote + '"');
    if (active && active.turn && active.turn.result && active.turn.result.trace_id) {
      parts.push("trace " + active.turn.result.trace_id);
    }
    return parts.join(" · ");
  }

  function renderChip(citation, number, claimIndex, citeIndex, options) {
    const o = options || {};
    const tagged = o.style === "tagged";
    const docCitation = isDocumentCitation(citation);
    const label = citationSummary(citation);
    const ticker = citation && citation.ticker ? String(citation.ticker) : "";
    const tag = tagged && ticker ? '<span class="cite-chip__tag">' + escapeHtml(ticker) + "</span>" : "";
    return '<button type="button" class="cite-chip' +
      (tagged ? " cite-chip--tagged" : "") +
      (docCitation ? " cite-chip--doc" : "") +
      '" data-claim="' + claimIndex + '" data-cite="' + citeIndex + '" data-n="' + number +
      '" data-document="' + escapeAttr(citationDocument(citation)) +
      '" data-section="' + escapeAttr(citationSection(citation)) +
      '" data-line="' + escapeAttr(citation && citation.line !== undefined && citation.line !== null ? citation.line : "") +
      (docCitation ? "" : ' aria-haspopup="dialog"') +
      ' aria-label="Open citation ' + number + ": " + escapeAttr(label) + '">' +
      '<span class="cite-chip__n">' + number + "</span>" + tag + "</button>";
  }

  function countCitations(result) {
    return asArray(result && result.envelope && result.envelope.claims)
      .reduce((total, claim) => total + asArray(claim && claim.citations).length, 0);
  }

  function citationFrom(result, claimIndex, citeIndex) {
    const claim = asArray(result && result.envelope && result.envelope.claims)[claimIndex];
    return claim ? asArray(claim.citations)[citeIndex] : null;
  }

  /* ------------------------------------------------------------------------
     Document index + API access
     ------------------------------------------------------------------------ */

  const documentStore = {
    docs: [],
    asOf: null,
    loaded: false,
    loading: null,
    error: null,
  };

  const docIndex = {
    byId: new Map(),
    byName: new Map(),
  };

  function normalizeDocument(doc) {
    const item = objectOr(doc);
    let rawSections = item.sections;
    if (!Array.isArray(rawSections) && item.outline) {
      rawSections = Array.isArray(item.outline.sections) ? item.outline.sections : item.outline;
    }
    if (!Array.isArray(rawSections)) rawSections = item.headings;
    const sections = asArray(rawSections).filter((section) => section && typeof section === "object").map((section) => ({
      id: section && section.id ? String(section.id) : slugify(section && section.title),
      title: section && section.title ? String(section.title) : "",
      level: Number(section && section.level) || 2,
      line: Number(section && section.line) || 0,
      end_line: section && section.end_line !== undefined && section.end_line !== null
        ? Number(section.end_line) : null,
    }));
    return Object.assign({}, item, {
      id: item.id ? String(item.id) : "",
      title: item.title ? String(item.title) : (item.path ? String(item.path).split("/").pop() : "Untitled note"),
      ticker: item.ticker || null,
      period: item.period || null,
      year: item.year || null,
      kind: item.kind || "other",
      tags: asArray(item.tags),
      aliases: asArray(item.aliases),
      sections: sections,
    });
  }

  function indexDocument(doc) {
    if (!doc.id) return;
    docIndex.byId.set(String(doc.id).toLowerCase(), doc);
    const names = [doc.id, doc.title, doc.path, (doc.path || "").split("/").pop()];
    asArray(doc.aliases).forEach((alias) => names.push(alias));
    names.filter(Boolean).forEach((name) => {
      const key = String(name).toLowerCase().replace(/\.md$/i, "").trim();
      if (key && !docIndex.byName.has(key)) docIndex.byName.set(key, doc);
    });
  }

  function buildIndex() {
    docIndex.byId.clear();
    docIndex.byName.clear();
    documentStore.docs.forEach(indexDocument);
  }

  function dispatch(name, detail) {
    try {
      window.dispatchEvent(new CustomEvent(name, { detail: detail }));
    } catch (error) {
      /* CustomEvent is available everywhere the app runs. */
    }
  }

  const documents = {
    state() {
      return documentStore;
    },

    load(force) {
      if (documentStore.loaded && !force) return Promise.resolve(documentStore.docs);
      if (documentStore.loading && !force) return documentStore.loading;
      documentStore.error = null;
      documentStore.loading = fetch("/api/documents", {
        headers: { Accept: "application/json" },
        cache: "no-store",
      }).then(async (response) => {
        const payload = await readJson(response);
        if (!response.ok) {
          throw new Error(errorMessage(payload, "Document index answered HTTP " + response.status + "."));
        }
        if (!payload || payload.ok === false) {
          throw new Error(errorMessage(payload, "Document index returned an error."));
        }
        const list = Array.isArray(payload) ? payload :
          (Array.isArray(payload.documents) ? payload.documents : []);
        documentStore.docs = list.map(normalizeDocument).filter((doc) => doc.id);
        documentStore.asOf = payload.as_of || null;
        documentStore.loaded = true;
        documentStore.error = null;
        documentStore.loading = null;
        buildIndex();
        dispatch("rd:documents", documentStore);
        return documentStore.docs;
      }).catch((error) => {
        documentStore.error = error;
        documentStore.loaded = false;
        documentStore.loading = null;
        dispatch("rd:documents", documentStore);
        throw error;
      });
      return documentStore.loading;
    },

    all() {
      return documentStore.docs;
    },

    byId(id) {
      if (!id) return null;
      const key = String(id).toLowerCase();
      return docIndex.byId.get(key) || null;
    },

    resolve(target) {
      if (!target) return null;
      let value = String(target).trim();
      let section = "";
      const hash = value.indexOf("#");
      if (hash >= 0) {
        section = value.slice(hash + 1).trim();
        value = value.slice(0, hash).trim();
      }
      const key = value.toLowerCase().replace(/\.md$/i, "").trim();
      const doc = docIndex.byId.get(key) || docIndex.byName.get(key) || null;
      if (!doc) return null;
      return {
        id: doc.id,
        document: doc,
        section: section ? (findSection(doc, section) || {}).id || slugify(section) : "",
      };
    },

    findSection(doc, sectionRef) {
      return findSection(doc, sectionRef);
    },

    title(id) {
      const doc = documents.byId(id);
      return doc ? doc.title : String(id || "Note");
    },
  };

  function findSection(doc, reference) {
    if (!doc || reference === undefined || reference === null || reference === "") return null;
    const sections = asArray(doc.sections);
    const ref = String(reference).trim();
    const lowered = ref.toLowerCase();
    const numeric = Number(ref);
    if (Number.isFinite(numeric)) {
      const exact = sections.find((section) => Number(section.line) === numeric);
      if (exact) return exact;
      const containing = sections
        .filter((section) => Number(section.line) <= numeric &&
          (section.end_line === null || section.end_line === undefined || numeric <= Number(section.end_line)))
        .sort((a, b) => Number(b.line) - Number(a.line))[0];
      if (containing) return containing;
    }
    return sections.find((section) => String(section.id).toLowerCase() === lowered) ||
      sections.find((section) => String(section.title).toLowerCase() === lowered) ||
      sections.find((section) => slugify(section.title) === slugify(ref)) ||
      sections.find((section) => String(section.title).toLowerCase().includes(lowered)) ||
      null;
  }

  async function getDocument(id) {
    const response = await fetch("/api/documents/" + encodeURIComponent(id), {
      headers: { Accept: "application/json" },
      cache: "no-store",
    });
    const payload = await readJson(response);
    if (!response.ok) {
      const error = new Error(errorMessage(payload, "The note answered HTTP " + response.status + "."));
      error.status = response.status;
      throw error;
    }
    if (!payload) throw new Error("The note endpoint returned an empty response.");
    if (payload.ok === false) {
      const error = new Error(errorMessage(payload, "The note could not be read."));
      error.status = 404;
      throw error;
    }
    const doc = payload.document ? payload.document : payload;
    const normalized = normalizeDocument(doc);
    if (payload.outline && !normalized.sections.length) {
      normalized.sections = asArray(payload.outline.sections ? payload.outline.sections : payload.outline)
        .map((section) => ({
          id: String(section.id || slugify(section.title)),
          title: String(section.title || ""),
          level: Number(section.level) || 2,
          line: Number(section.line) || 0,
          end_line: section.end_line !== undefined && section.end_line !== null
            ? Number(section.end_line) : null,
        }));
    }
    normalized.content = String(payload.content !== undefined ? payload.content : normalized.content || "");
    normalized.frontmatter = payload.frontmatter || normalized.frontmatter || null;
    normalized.sha256 = payload.sha256 || normalized.sha256 || "";
    return normalized;
  }

  function docHref(id, options) {
    const o = options || {};
    const params = new URLSearchParams();
    if (o.section) params.set("section", o.section);
    if (o.q) params.set("q", o.q);
    if (o.from) params.set("from", o.from);
    if (o.quote) params.set("quote", String(o.quote).slice(0, MAX_QUOTE_LENGTH));
    const query = params.toString();
    return "#/doc/" + encodeURIComponent(id) + (query ? "?" + query : "");
  }

  function deskHref(options) {
    const o = options || {};
    const params = new URLSearchParams();
    if (o.q) params.set("q", o.q);
    if (o.ticker) params.set("ticker", o.ticker);
    if (o.period) params.set("period", o.period);
    if (o.year) params.set("year", String(o.year));
    if (o.from) params.set("from", o.from);
    if (o.doc) params.set("doc", o.doc);
    if (o.verify) params.set("verify", "1");
    const query = params.toString();
    return "#/desk" + (query ? "?" + query : "");
  }

  function navigate(hash) {
    const value = String(hash || "");
    const next = value.charAt(0) === "#" ? value : "#" + value;
    if (window.location.hash === next) {
      if (typeof RD.rerender === "function") RD.rerender();
      return;
    }
    window.location.hash = next;
  }

  async function postAsk(payload, options) {
    const o = options || {};
    const controller = new AbortController();
    const timer = window.setTimeout(() => controller.abort(), o.timeout || CLIENT_TIMEOUT_MS);
    try {
      const response = await fetch("/api/ask", {
        method: "POST",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify(payload),
        signal: controller.signal,
      });
      const body = await readJson(response);
      if (!response.ok) {
        throw new Error(errorMessage(body, "The desk answered HTTP " + response.status + "."));
      }
      if (!body || typeof body !== "object") {
        throw new Error("The desk returned an empty response.");
      }
      if (typeof body.status !== "string") {
        throw new Error("The desk returned an unrecognised response.");
      }
      return body;
    } catch (error) {
      if (error && error.name === "AbortError") {
        const timeout = new Error("timeout");
        timeout.isTimeout = true;
        throw timeout;
      }
      throw error;
    } finally {
      window.clearTimeout(timer);
    }
  }

  /* ------------------------------------------------------------------------
     Answer envelope rendering
     ------------------------------------------------------------------------ */

  function resultStatus(result) {
    const status = String((result && result.status) || "").toLowerCase();
    if (status === "answered" || status === "ok" || status === "success") return "answered";
    if (status === "refused") return "refused";
    if (status === "unsupported") return "unsupported";
    return "error";
  }

  function renderMissing(missing, options) {
    const entry = missing || {};
    const o = options || {};
    const state = String(entry.state || "unknown");
    const evidence = objectOr(entry.evidence);
    const rows = Object.entries(evidence).filter(([, value]) => {
      if (value === null || value === undefined || value === "") return false;
      if (Array.isArray(value) && value.length === 0) return false;
      if (typeof value === "object" && !Array.isArray(value) && Object.keys(value).length === 0) return false;
      return true;
    });

    const silent = state === "not_in_document";
    const detail = rows.length
      ? '<dl class="evidence">' + rows.map(([key, value]) =>
          "<dt>" + escapeHtml(humanKey(key)) + "</dt><dd>" + evidenceValue(key, value) + "</dd>"
        ).join("") + "</dl>"
      : "";
    const actions = typeof o.missingActions === "function" ? o.missingActions(entry) : "";
    const silentNote = silent
      ? '<p class="missing__quiet">The pinned note is silent on this item. Silence in the note is not proof the filing omits it — check the filings or ask the desk.</p>'
      : "";

    return '<div class="missing' + (silent ? " missing--silent" : "") + '" role="note">' +
      '<div class="missing__head">' +
        '<span class="missing__stamp">' + (silent ? "Not in note" : "Missing") + "</span>" +
        '<span class="missing__item">' + escapeHtml(entry.item || "unspecified item") + "</span>" +
        '<span class="state-chip state-chip--' + escapeAttr(state) + '">' + escapeHtml(humanState(state)) + "</span>" +
      "</div>" + silentNote + detail +
      (actions ? '<div class="missing__actions">' + actions + "</div>" : "") +
    "</div>";
  }

  function evidenceValue(key, value) {
    if (Array.isArray(value)) {
      return escapeHtml(value.map((item) => typeof item === "string" ? item : JSON.stringify(item)).join(" · "));
    }
    if (value && typeof value === "object") {
      try {
        return "<code>" + escapeHtml(JSON.stringify(value)) + "</code>";
      } catch (error) {
        return escapeHtml(String(value));
      }
    }
    const text = String(value);
    if (/^https?:\/\//i.test(text) && /(url|source)/i.test(key)) {
      return '<a href="' + escapeAttr(text) + '" target="_blank" rel="noopener noreferrer">' + escapeHtml(text) + "</a>";
    }
    return escapeHtml(text);
  }

  function renderCoverage(coverage, options) {
    const o = options || {};
    if (!coverage) return "";
    if (typeof coverage !== "object" || Array.isArray(coverage)) {
      return '<section class="coverage-banner" aria-label="Coverage">' +
        '<div class="coverage-banner__head"><span class="coverage-banner__stamp">Coverage</span>' +
        '<p class="coverage-banner__scope">' + escapeHtml(String(coverage)) + "</p></div></section>";
    }
    const scope = String(coverage.scope || "supported_set");
    const supported = coverage.supported_tickers;
    const compared = asArray(coverage.compared_tickers).map(String);
    const excluded = asArray(coverage.excluded);
    const requested = coverage.requested;
    const answered = coverage.answered;
    const caveat = coverage.caveat ||
      (scope === "supported_set"
        ? "Supported names only; this is not an IDX-wide sector screen."
        : "Coverage reflects the documents and tools actually used.");

    const scopeLine = scope === "supported_set"
      ? "Supported set only — not IDX-wide"
      : humanState(scope);
    const counts = (requested !== undefined || answered !== undefined)
      ? (answered !== undefined ? answered : "—") + " of " +
        (requested !== undefined ? requested : "—") + " requested answered"
      : "";

    const tickerChips = compared.length
      ? '<div class="coverage-banner__tickers"><span class="coverage-banner__label">Compared</span>' +
        compared.map((ticker) => '<span class="chip chip--ticker">' + escapeHtml(ticker) + "</span>").join("") +
        "</div>"
      : "";

    const supportedLine = Array.isArray(supported)
      ? supported.length + " issuers in the supported set"
      : (supported ? supported + " issuers in the supported set" : "");

    const excludedBlock = excluded.length
      ? '<div class="coverage-banner__excluded"><p class="coverage-banner__label">Excluded</p><ul>' +
        excluded.map((item) => {
          const entry = typeof item === "object" && item ? item : { reason: String(item) };
          return "<li><b>" + escapeHtml(entry.ticker || "—") + "</b>" +
            (entry.metric ? " · " + escapeHtml(entry.metric) : "") +
            (entry.reason ? " — " + escapeHtml(entry.reason) : "") + "</li>";
        }).join("") + "</ul></div>"
      : "";

    return '<section class="coverage-banner' + (o.compact ? " coverage-banner--compact" : "") +
      '" aria-label="Coverage">' +
      '<div class="coverage-banner__head">' +
        '<span class="coverage-banner__stamp">Coverage</span>' +
        '<p class="coverage-banner__scope">' + escapeHtml(scopeLine) +
          (supportedLine ? ' <span class="coverage-banner__sep">·</span> ' + escapeHtml(supportedLine) : "") +
        "</p>" +
        (counts ? '<span class="coverage-banner__counts">' + escapeHtml(counts) + "</span>" : "") +
      "</div>" +
      '<p class="coverage-banner__caveat">' + escapeHtml(caveat) + "</p>" +
      tickerChips + excludedBlock +
    "</section>";
  }

  function formatCellValue(value) {
    if (value === undefined || value === null || value === "") return "—";
    if (typeof value === "number") return formatNumber(value);
    return String(value);
  }

  function renderComparison(matrix, options) {
    if (!matrix) return "";
    const columns = asArray(matrix.columns).map(String);
    const rows = asArray(matrix.rows);
    if (!columns.length || !rows.length) return "";
    const period = [matrix.period, matrix.year].filter(Boolean).join(" ");
    const excluded = asArray(matrix.excluded);
    const coverage = objectOr(matrix.coverage);

    const head = "<tr><th scope=\"col\" class=\"matrix__metric-head\">Metric</th>" +
      columns.map((ticker) => '<th scope="col">' + escapeHtml(ticker) + "</th>").join("") + "</tr>";

    const body = rows.map((row) => {
      const entry = row || {};
      const cells = asArray(entry.cells);
      const values = objectOr(entry.values);
      const cellsHtml = columns.map((ticker) => {
        const cell = cells.find((candidate) => candidate && String(candidate.ticker) === ticker) ||
          (values[ticker] !== undefined ? { ticker: ticker, value: values[ticker] } : null);
        if (!cell) {
          return '<td class="matrix__cell matrix__cell--missing"><span class="state-chip">not returned</span></td>';
        }
        const status = String(cell.status || "ok");
        if (status !== "ok" || cell.value === undefined || cell.value === null) {
          const reason = cell.evidence && (cell.evidence.error || cell.evidence.url);
          return '<td class="matrix__cell matrix__cell--missing" title="' +
            escapeAttr(reason || humanState(status)) + '"><span class="state-chip state-chip--' +
            escapeAttr(status) + '">' + escapeHtml(humanState(status)) + "</span></td>";
        }
        return '<td class="matrix__cell">' + escapeHtml(formatCellValue(cell.value)) +
          (cell.period_type ? '<span class="matrix__pt">' + escapeHtml(cell.period_type) + "</span>" : "") +
          "</td>";
      }).join("");
      return "<tr><th scope=\"row\">" + escapeHtml(entry.metric || "—") +
        (entry.unit ? '<span class="matrix__unit">' + escapeHtml(entry.unit) + "</span>" : "") +
        "</th>" + cellsHtml + "</tr>";
    }).join("");

    const coverageLine = coverage.requested !== undefined
      ? '<p class="matrix__coverage">' + escapeHtml(String(coverage.answered !== undefined ? coverage.answered : "—")) +
        " of " + escapeHtml(String(coverage.requested)) + " requested cells answered · " +
        escapeHtml(humanState(coverage.scope || "supported_set")) + "</p>"
      : "";

    const excludedLine = excluded.length
      ? '<ul class="matrix__excluded">' + excluded.map((item) => {
          const entry = typeof item === "object" && item ? item : { reason: String(item) };
          return "<li>" + escapeHtml(entry.ticker || "—") +
            (entry.metric ? " / " + escapeHtml(entry.metric) : "") +
            " — " + escapeHtml(entry.reason || "not applicable") + "</li>";
        }).join("") + "</ul>"
      : "";

    return '<section class="matrix-block" aria-label="Comparison matrix">' +
      '<div class="matrix-block__head">' +
        '<p class="matrix-block__title">Comparison matrix' + (period ? " · " + escapeHtml(period) : "") + "</p>" +
        '<span class="matrix-block__note">cited · no ranking</span>' +
      "</div>" +
      '<div class="table-wrap"><table class="matrix"><thead>' + head + "</thead><tbody>" + body + "</tbody></table></div>" +
      coverageLine + excludedLine +
    "</section>";
  }

  function collectComparisons(root) {
    const found = [];
    const seen = new WeakSet();
    let nodes = 0;

    function signature(matrix) {
      try {
        return JSON.stringify({ columns: matrix.columns, rows: asArray(matrix.rows).map((row) => row && row.metric) });
      } catch (error) {
        return "";
      }
    }

    function visit(value, depth) {
      if (!value || typeof value !== "object" || depth > 8) return;
      if (seen.has(value)) return;
      seen.add(value);
      nodes += 1;
      if (nodes > 1200) return;

      if (Array.isArray(value)) {
        value.forEach((entry) => visit(entry, depth + 1));
        return;
      }

      if (Array.isArray(value.columns) && Array.isArray(value.rows) &&
          value.rows.some((row) => row && Array.isArray(row.cells))) {
        const key = signature(value);
        if (key && !found.some((matrix) => signature(matrix) === key)) found.push(value);
      }

      Object.keys(value).forEach((key) => {
        if (key === "verification" || key === "claims") return;
        visit(value[key], depth + 1);
      });
    }

    visit(root, 0);
    return found;
  }

  function parseToolPayload(value, depth) {
    const level = depth || 0;
    if (level > 4 || value === undefined || value === null) return value;
    if (typeof value === "string") {
      const text = value.trim();
      if (!text || (text.charAt(0) !== "{" && text.charAt(0) !== "[")) return value;
      try {
        return JSON.parse(text);
      } catch (error) {
        return value;
      }
    }
    if (Array.isArray(value)) {
      for (const item of value) {
        const parsed = parseToolPayload(item, level + 1);
        if (parsed && typeof parsed === "object") {
          if (Array.isArray(parsed.columns) || parsed.data || parsed.ok !== undefined) return parsed;
          if (parsed.text !== undefined) return parseToolPayload(parsed.text, level + 1);
        }
      }
      return value;
    }
    if (typeof value === "object") {
      if (Array.isArray(value.columns) && Array.isArray(value.rows)) return value;
      if (Array.isArray(value.content)) {
        const parsed = parseToolPayload(value.content, level + 1);
        if (parsed !== value.content) return parsed;
      }
      if (value.data !== undefined) {
        const parsed = parseToolPayload(value.data, level + 1);
        if (parsed && parsed !== value.data) return parsed;
      }
      if (value.result !== undefined && value.result !== value) {
        const parsed = parseToolPayload(value.result, level + 1);
        if (parsed && parsed !== value.result) return parsed;
      }
      if (value.text !== undefined) {
        const parsed = parseToolPayload(value.text, level + 1);
        if (parsed !== value.text) return parsed;
      }
    }
    return value;
  }

  function comparisonsFromTrace(trace) {
    const found = [];
    const seen = new Set();
    asArray(trace && trace.tool_trace).forEach((entry) => {
      if (!entry || typeof entry !== "object") return;
      const name = entry.tool_name || entry.tool || entry.name;
      if (name !== "compare_supported") return;
      let payload = entry.result;
      if (payload === undefined) payload = entry.output;
      if (payload === undefined) payload = entry.response;
      if (payload === undefined) payload = entry.data;
      if (payload === undefined) return;
      payload = parseToolPayload(payload, 0);
      let data = payload && payload.data ? payload.data : payload;
      if (data && data.result && data.result.data) data = data.result.data;
      if (!data || !Array.isArray(data.columns) || !Array.isArray(data.rows) || !data.columns.length) return;
      let signature = "";
      try {
        signature = JSON.stringify({ columns: data.columns, rows: data.rows.map((row) => row && row.metric) });
      } catch (error) {
        signature = "";
      }
      if (signature && seen.has(signature)) return;
      if (signature) seen.add(signature);
      found.push(data);
    });
    return found;
  }

  function renderAuditNote(verification) {
    const violations = asArray(verification && verification.violations);
    if (!violations.length || verification.ok === false) return "";
    return '<div class="audit-note"><strong>Audit note</strong> ' + violations.length +
      " violation" + (violations.length === 1 ? "" : "s") +
      " recorded in the trace: " +
      escapeHtml(violations.map((item) => violationLabel(item && item.type)).join(", ")) + ".</div>";
  }

  function verdictFooter(result) {
    const verification = objectOr(result && result.verification);
    const citations = objectOr(verification.citations);
    const usage = objectOr(result && result.usage);
    const passed = verification.ok !== false;
    const totalCitations = countCitations(result);
    const okCitations = Number.isFinite(Number(citations.ok)) ? Number(citations.ok) : totalCitations;
    const removed = Number.isFinite(Number(citations.removed)) ? Number(citations.removed) : 0;

    let stamp = "Verified";
    let stampClass = "";
    if (resultStatus(result) === "refused") {
      stamp = "Refused";
      stampClass = "stamp--refused";
    } else if (!passed) {
      stamp = "Audit flag";
      stampClass = "stamp--reported";
    }

    const target = objectOr(result && result.document);
    return '<footer class="verdict" aria-label="Verification summary">' +
      '<span class="stamp ' + stampClass + '">' + stamp + "</span>" +
      '<dl class="meta-strip">' +
        "<div><dt>Citations</dt><dd>" + escapeHtml(okCitations + " ok" + (removed ? " · " + removed + " removed" : "")) + "</dd></div>" +
        "<div><dt>Cost</dt><dd>" + escapeHtml(formatCost(usage.cost_usd)) + "</dd></div>" +
        "<div><dt>Latency</dt><dd>" + escapeHtml(formatDuration(result && result.duration_s)) + "</dd></div>" +
        "<div><dt>Model</dt><dd>" + escapeHtml((result && result.model) || "—") + "</dd></div>" +
        '<div><dt>Trace</dt><dd>' + escapeHtml(String((result && result.trace_id) || "—")) + "</dd></div>" +
        (result && result.mode ? "<div><dt>Mode</dt><dd>" + escapeHtml(result.mode) + "</dd></div>" : "") +
        (target.id ? "<div><dt>Note</dt><dd>" + escapeHtml(target.id) + "</dd></div>" : "") +
      "</dl>" +
    "</footer>";
  }

  function renderAnswered(result, options) {
    const o = options || {};
    const envelope = objectOr(result.envelope);
    const answer = typeof envelope.answer === "string" ? envelope.answer : "";
    const claims = asArray(envelope.claims).filter((claim) => claim && typeof claim === "object");
    const missing = asArray(envelope.missing).filter(Boolean);
    let citationNumber = 0;

    const claimItems = claims.map((claim, claimIndex) => {
      const type = normalizeClaimType(claim.type);
      const citations = asArray(claim.citations).filter(Boolean);
      const chips = citations.map((citation, citeIndex) => {
        citationNumber += 1;
        return renderChip(citation, citationNumber, claimIndex, citeIndex, {
          style: o.chipStyle || "round",
        });
      }).join("");
      const sources = chips
        ? '<div class="cite-row"><span class="cite-row__label">Sources</span>' + chips + "</div>"
        : '<div class="cite-row cite-row--none">No citation filed — flagged for review</div>';

      return '<li class="claim claim--' + type + '">' +
        '<div class="claim__top">' +
          '<span class="badge badge--' + type + '" title="' + escapeAttr(badgeDescription(type)) + '">' +
            badgeLabel(type) + "</span>" +
          '<span class="claim__no">' + pad2(claimIndex + 1) + "</span>" +
        "</div>" +
        '<p class="claim__text">' + renderInline(String(claim.text || ""), o.inline || {}, 0) + "</p>" +
        sources +
      "</li>";
    }).join("");

    const claimsSection = claims.length
      ? '<section class="claims" aria-label="Claims on the record">' +
          '<p class="claims__label">Claims on the record <span>' + claims.length +
            " claim" + (claims.length === 1 ? "" : "s") + "</span></p>" +
          "<ol>" + claimItems + "</ol>" +
        "</section>"
      : "";

    const missingSection = missing.map((entry) => renderMissing(entry, o)).join("");
    const answerBlock = answer
      ? '<div class="answer__prose">' + renderMarkdown(answer, { links: true, images: false }) + "</div>"
      : '<p class="prose-empty">The answer envelope carried no prose. Claims are listed below.</p>';

    const coverage = o.coverage === false ? "" : renderCoverage(envelope.coverage || result.coverage, o);
    const matrices = o.matrix === false ? "" :
      collectComparisons({ envelope: envelope, tool_calls: result.tool_calls, comparison: result.comparison })
        .map((matrix) => renderComparison(matrix, o)).join("");

    const actions = typeof o.actions === "function" ? o.actions(result, envelope) : (o.actionsHtml || "");
    const audit = renderAuditNote(objectOr(result.verification));
    const footer = o.footer === false ? "" : verdictFooter(result);

    return '<div class="answer answer--' + escapeAttr(o.mode || "desk") + '">' +
      answerBlock + claimsSection + missingSection + coverage + matrices + actions + audit + footer +
    "</div>";
  }

  function renderRefused(result) {
    const verification = objectOr(result && result.verification);
    const violations = asArray(verification.violations);
    const list = violations.length
      ? '<ol class="violations">' + violations.map((violation) => {
          const item = violation || {};
          return "<li>" +
            '<div class="violation__head">' +
              '<span class="violation__type">' + escapeHtml(violationLabel(item.type)) + "</span>" +
              (item.token !== undefined && item.token !== null && String(item.token) !== ""
                ? '<span class="violation__token">' + escapeHtml(String(item.token)) + "</span>"
                : "") +
            "</div>" +
            (item.context ? '<p class="violation__context">' + escapeHtml(String(item.context)) + "</p>" : "") +
          "</li>";
        }).join("") + "</ol>"
      : '<p class="panel__body">The refusal carried no violation detail.</p>';
    const errorBlock = result && result.error
      ? '<p class="panel__error">' + escapeHtml(String(result.error)) + "</p>"
      : "";
    return '<div class="panel panel--refused">' +
      '<span class="panel__stamp">Refused</span>' +
      '<h3 class="panel__title">Unverified numbers withheld.</h3>' +
      '<p class="panel__body">The number audit stopped this answer. No prose from the run is shown: a figure that cannot be traced to a tool ledger does not reach the record.</p>' +
      list + errorBlock +
    "</div>" + verdictFooter(result);
  }

  function renderErrorPanel(result, override) {
    const message = override || (result && result.error) || "The run ended without a message.";
    const lowered = String(message).toLowerCase();
    let stamp = "Run error";
    let title = "The run did not complete.";
    let body = "Nothing partial is shown. The trace may still hold the tool calls that ran.";

    if (lowered.includes("timeout")) {
      stamp = "Timeout";
      title = "The desk timed out.";
      body = "The 180-second budget expired before the audit finished. No partial answer is shown; retry when the runner is quiet. The note itself stays readable.";
    } else if (lowered.includes("404") || lowered.includes("not found") ||
      lowered.includes("missing note") || lowered.includes("document not found")) {
      stamp = "Missing";
      title = "That record is not on file.";
      body = "The server could not find this note. Return to the library or ask the desk about the issuer.";
    } else if (lowered.includes("no module") || lowered.includes("runner") || lowered.includes("agent")) {
      stamp = "Runner";
      title = "The model runner is unavailable.";
      body = "The local pi runner did not answer. Check the server log, then file the question again.";
    } else if (lowered.includes("network") || lowered.includes("failed to fetch") || lowered.includes("offline")) {
      stamp = "Offline";
      title = "The desk is offline.";
      body = "The browser could not reach the local server. Start the app on 127.0.0.1:8765, then retry.";
    }

    return '<div class="panel panel--error">' +
      '<span class="panel__stamp">' + escapeHtml(stamp) + "</span>" +
      '<h3 class="panel__title">' + escapeHtml(title) + "</h3>" +
      '<p class="panel__body">' + escapeHtml(body) + "</p>" +
      '<p class="panel__error">' + escapeHtml(String(message)) + "</p>" +
      '<div class="panel__actions"><button type="button" class="button-quiet" data-retry>Retry question</button></div>' +
    "</div>";
  }

  function collectAlternatives(result) {
    const found = new Set();
    const keys = new Set(["alternatives", "supported_tickers", "supported_periods", "supported"]);
    const seen = new WeakSet();

    function walk(value, depth) {
      if (!value || typeof value !== "object" || depth > 5) return;
      if (seen.has(value)) return;
      seen.add(value);
      Object.keys(value).forEach((key) => {
        const item = value[key];
        if (keys.has(key) && Array.isArray(item)) {
          item.forEach((entry) => {
            if (typeof entry === "string" && entry.trim()) found.add(entry.trim());
          });
          return;
        }
        if (Array.isArray(item)) {
          item.forEach((entry) => walk(entry, depth + 1));
          return;
        }
        if (item && typeof item === "object") walk(item, depth + 1);
      });
    }

    walk(result, 0);
    return Array.from(found);
  }

  function renderUnsupported(result) {
    const alternatives = collectAlternatives(result);
    const message = (result && (result.error || result.message)) ||
      "That issuer or period is not in the supported fact-sheet set.";
    const chips = alternatives.length
      ? '<p class="panel__body">Supported alternatives:</p><div class="alt-chips">' +
        alternatives.slice(0, 30).map((item) => '<span class="alt-chip">' + escapeHtml(item) + "</span>").join("") +
        "</div>"
      : "";
    return '<div class="panel panel--unsupported">' +
      '<span class="panel__stamp">Unsupported</span>' +
      '<h3 class="panel__title">Outside the supported set.</h3>' +
      '<p class="panel__body">' + escapeHtml(String(message)) + "</p>" +
      chips +
      '<div class="panel__actions">' +
        '<a class="button-quiet" href="#/documents">Open the vault →</a>' +
        '<a class="button-quiet" href="#/desk">Ask the desk →</a>' +
      "</div>" +
    "</div>";
  }

  function renderResult(result, options) {
    const status = resultStatus(result);
    if (status === "answered") return renderAnswered(result, options);
    if (status === "refused") return renderRefused(result);
    if (status === "unsupported") return renderUnsupported(result);
    return renderErrorPanel(result);
  }

  function renderPending(turnId, options) {
    const o = options || {};
    return '<div class="pending" aria-hidden="false">' +
      '<span class="pending__scan" aria-hidden="true"></span>' +
      '<div class="pending__top">' +
        '<p class="pending__line">' + escapeHtml(o.label || "Reading filings") +
          '<span class="pending__caret" aria-hidden="true"></span></p>' +
        '<span class="pending__elapsed" data-elapsed-turn="' + escapeAttr(String(turnId)) + '">0.0 s</span>' +
      "</div>" +
      '<p class="pending__note">' + escapeHtml(o.note ||
        "Checking the fact sheets, captured releases and notes. Prose without a ledger will not be shown.") +
      "</p></div>";
  }

  function resultSummary(result) {
    if (!result) return "No result.";
    const status = resultStatus(result);
    if (status === "answered") {
      const envelope = objectOr(result.envelope);
      const answer = typeof envelope.answer === "string" ? envelope.answer : "";
      const claims = asArray(envelope.claims).map((claim) => claim && claim.text).filter(Boolean).join(" ");
      return "Answered: " + (answer || claims || "no prose returned.");
    }
    if (status === "refused") {
      const tokens = asArray(result.verification && result.verification.violations)
        .map((violation) => violation && violation.token)
        .filter(Boolean)
        .join(", ");
      return "Refused by the audit gate" + (tokens ? " (offending numbers: " + tokens + ")" : "") + ".";
    }
    if (status === "unsupported") {
      return "Unsupported scope: " + (result.error || result.message || "not in the supported set") + ".";
    }
    return "Run error: " + (result.error || "unknown error") + ".";
  }

  function scopeText(scope) {
    if (!scope) return "all supported";
    if (!scope.ticker) return scope.label || "all supported";
    return scope.ticker + " " + (scope.periodLabel || scope.period || "latest");
  }

  function historyFrom(turns) {
    const messages = [];
    asArray(turns).filter((turn) => !turn.pending && turn.result).forEach((turn) => {
      messages.push({ role: "user", content: trimText(turn.question, 1200) });
      messages.push({
        role: "assistant",
        content: trimText("[scope: " + scopeText(turn.scope) + "] " + resultSummary(turn.result), 1600),
      });
    });
    return messages.slice(-MAX_HISTORY_MESSAGES);
  }

  function trimText(value, limit) {
    const text = String(value === undefined || value === null ? "" : value);
    return text.length > limit ? text.slice(0, limit) + "…" : text;
  }

  function announceResult(result) {
    const status = resultStatus(result);
    if (status === "answered") {
      const claims = asArray(result.envelope && result.envelope.claims).length;
      const citations = countCitations(result);
      const missing = asArray(result.envelope && result.envelope.missing).length;
      announce("Answer filed. " + claims + " claim" + (claims === 1 ? "" : "s") + ", " +
        citations + " citation" + (citations === 1 ? "" : "s") +
        (missing ? ", " + missing + " missing-data note" + (missing === 1 ? "" : "s") : "") + ".");
      return;
    }
    if (status === "refused") {
      announce("Answer refused. Unverified numbers were withheld.");
      return;
    }
    if (status === "unsupported") {
      announce("That scope is outside the supported set.");
      return;
    }
    announce("The run failed. " + ((result && result.error || "Unknown error") + "."));
  }

  /* ------------------------------------------------------------------------
     Clipboard
     ------------------------------------------------------------------------ */

  async function copyText(text) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      await navigator.clipboard.writeText(text);
      return;
    }
    const area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.style.position = "fixed";
    area.style.opacity = "0";
    document.body.append(area);
    area.select();
    const ok = document.execCommand("copy");
    area.remove();
    if (!ok) throw new Error("copy command failed");
  }

  /* ------------------------------------------------------------------------
     Evidence inspector
     ------------------------------------------------------------------------ */

  const inspectorState = {
    traceCache: new Map(),
    token: 0,
    active: null,
    lastFocus: null,
  };

  const inspectorDom = {
    inspector: null,
    close: null,
    title: null,
    sub: null,
    detail: null,
    trace: null,
    copy: null,
    copyStatus: null,
    scrim: null,
    noteLink: null,
  };

  async function loadTrace(traceId) {
    if (inspectorState.traceCache.has(traceId)) {
      const cached = inspectorState.traceCache.get(traceId);
      if (cached instanceof Error) throw cached;
      return cached;
    }
    const request = fetch("/api/trace/" + encodeURIComponent(traceId), {
      headers: { Accept: "application/json" },
      cache: "no-store",
    }).then(async (response) => {
      if (!response.ok) throw new Error("trace endpoint answered " + response.status);
      return readJson(response);
    });
    inspectorState.traceCache.set(traceId, request);
    try {
      return await request;
    } catch (error) {
      inspectorState.traceCache.delete(traceId);
      throw error;
    }
  }

  function traceFallback(turn, note) {
    const result = (turn && turn.result) || {};
    const verification = objectOr(result.verification);
    return '<p class="trace__error">' + escapeHtml(note) + "</p>" +
      renderTrace({
        status: result.status,
        mode: result.mode,
        model: result.model,
        duration_s: result.duration_s,
        usage: result.usage,
        verification: { ok: verification.ok, violations: asArray(verification.violations) },
        tool_calls: asArray(result.tool_calls),
      }, turn, true);
  }

  function deriveLedger(trace) {
    const rows = [];
    asArray(trace && trace.tool_trace).forEach((call) => {
      asArray(call && call.ledger).forEach((entry) => rows.push(entry));
    });
    asArray(trace && trace.ledgers).forEach((ledger) => {
      asArray(ledger && ledger.entries).forEach((entry) => rows.push(entry));
    });
    return rows;
  }

  function formatLedgerValue(entry) {
    const item = entry || {};
    const value = typeof item.value === "number" ? formatNumber(item.value) :
      String(item.value === undefined || item.value === null ? "—" : item.value);
    return item.unit ? value + " " + item.unit : value;
  }

  function renderTrace(trace, turn, synthetic) {
    const source = trace || {};
    const result = (turn && turn.result) || {};
    const verification = objectOr(source.verification && Object.keys(source.verification).length
      ? source.verification
      : result.verification);
    const violations = asArray(verification.violations).length
      ? asArray(verification.violations)
      : asArray(source.violations);
    const ledger = asArray(source.ledger).length ? asArray(source.ledger) : deriveLedger(source);
    const toolCalls = asArray(source.tool_calls).length
      ? asArray(source.tool_calls)
      : Array.from(new Set(asArray(source.tool_trace).map((item) => item && (item.tool || item.tool_name)).filter(Boolean)));
    const status = source.status || result.status || "recorded";
    const usage = source.usage || result.usage || {};

    const summary = '<dl class="trace__grid">' +
      "<div><dt>Status</dt><dd>" + escapeHtml(String(status)) + "</dd></div>" +
      "<div><dt>Verdict</dt><dd>" + escapeHtml(verification.ok === false ? "violations recorded" : "passed") + "</dd></div>" +
      "<div><dt>Duration</dt><dd>" + escapeHtml(formatDuration(source.duration_s !== undefined ? source.duration_s : result.duration_s)) + "</dd></div>" +
      "<div><dt>Cost</dt><dd>" + escapeHtml(formatCost(usage.cost_usd)) + "</dd></div>" +
      "<div><dt>Model</dt><dd>" + escapeHtml(source.model || result.model || "—") + "</dd></div>" +
      "<div><dt>Mode</dt><dd>" + escapeHtml(source.mode || result.mode || "—") + "</dd></div>" +
      (source.document_id ? "<div><dt>Note</dt><dd>" + escapeHtml(source.document_id) + "</dd></div>" : "") +
      "<div><dt>Ledger</dt><dd>" + ledger.length + " entries</dd></div>" +
    "</dl>";

    const tools = toolCalls.length
      ? '<div class="trace__tools" aria-label="Tool calls">' + toolCalls.slice(0, 12).map((tool) =>
          '<span class="trace__tool">' + escapeHtml(typeof tool === "string" ? tool : JSON.stringify(tool)) + "</span>"
        ).join("") + "</div>"
      : "";

    const violationBlock = violations.length
      ? '<ol class="violations">' + violations.map((violation) => {
          const item = violation || {};
          return "<li>" +
            '<div class="violation__head">' +
              '<span class="violation__type">' + escapeHtml(violationLabel(item.type)) + "</span>" +
              (item.token !== undefined && item.token !== null && String(item.token) !== ""
                ? '<span class="violation__token">' + escapeHtml(String(item.token)) + "</span>"
                : "") +
            "</div>" +
            (item.context ? '<p class="violation__context">' + escapeHtml(String(item.context)) + "</p>" : "") +
          "</li>";
        }).join("") + "</ol>"
      : '<p class="trace__quiet">No audit violations recorded.</p>';

    const ledgerBlock = ledger.length
      ? '<ul class="trace__list">' + ledger.slice(0, 30).map((entry) => {
          const item = entry || {};
          const meta = objectOr(item.meta);
          const label = [meta.metric || item.kind || "value", meta.ticker, meta.period, meta.year]
            .filter((value) => value !== undefined && value !== null && String(value) !== "")
            .join(" · ");
          return '<li class="trace__row">' +
            '<span class="trace__row-main"><b>' + escapeHtml(item.kind || "value") + "</b> " + escapeHtml(label) + "</span>" +
            '<span class="trace__row-value">' + escapeHtml(formatLedgerValue(item)) + "</span>" +
            (item.source ? '<span class="trace__row-source">' + escapeHtml(String(item.source)) + "</span>" : "") +
          "</li>";
        }).join("") + "</ul>" +
        (ledger.length > 30 ? '<p class="trace__more">' + (ledger.length - 30) + " further ledger entries not shown.</p>" : "")
      : '<p class="trace__quiet">No ledger entries were stored for this run.</p>';

    return summary + tools +
      '<h4 class="inspector__label trace__subhead">Violations</h4>' + violationBlock +
      '<h4 class="inspector__label trace__subhead">Ledger</h4>' + ledgerBlock +
      (synthetic ? '<p class="trace__more">Source: run response (trace file unavailable).</p>' : "");
  }

  async function fillTrace(turn, token) {
    const result = (turn && turn.result) || {};
    const traceId = result.trace_id;
    if (!traceId) {
      inspectorDom.trace.innerHTML = traceFallback(turn, "This run did not return a trace id. Showing the response audit only.");
      return;
    }
    try {
      const trace = await loadTrace(traceId);
      if (token !== inspectorState.token) return;
      inspectorDom.trace.innerHTML = renderTrace(trace, turn);
    } catch (error) {
      if (token !== inspectorState.token) return;
      inspectorDom.trace.innerHTML = traceFallback(
        turn,
        "Trace " + traceId + " could not be read (" + (error.message || "unknown error") + "). Showing the response audit only."
      );
    }
  }

  function findDocForCitation(citation) {
    const id = citationDocument(citation);
    if (!id) return null;
    return documents.byId(id);
  }

  function openInspector(active) {
    if (!inspectorDom.inspector) initInspector();
    const citation = (active && active.citation) || {};
    inspectorState.lastFocus = (active && active.trigger) || document.activeElement;
    inspectorState.active = active;
    inspectorState.token += 1;
    const token = inspectorState.token;

    inspectorDom.title.textContent = "Citation " + pad2(active.number);
    inspectorDom.sub.textContent = citationSummary(citation);
    inspectorDom.copyStatus.textContent = "";
    inspectorDom.copy.disabled = false;

    const doc = findDocForCitation(citation);
    const section = doc ? findSection(doc, citationSection(citation) || citation.line) : null;
    const rows = [
      ["Tool", citation.tool],
      ["Issuer", citation.ticker],
      ["Metric", citation.metric],
      ["Driver", citation.driver],
      ["Document", citation.doc],
      ["Note", citationDocument(citation)],
      ["Section", citation.section || (section ? section.title : "")],
      ["Line", citation.line],
      ["Page", citation.page],
      ["Row", citation.row],
      ["Claim", "#" + pad2(active.claimIndex + 1)],
      ["Trace", active.turn && active.turn.result && active.turn.result.trace_id],
    ].filter(([, value]) => value !== undefined && value !== null && String(value) !== "");

    const quote = citation.quote
      ? '<blockquote class="inspector__quote">' + escapeHtml(String(citation.quote)) + "</blockquote>"
      : "";
    const noteLink = doc
      ? '<a class="button-quiet" data-open-cited-note href="' + escapeAttr(docHref(doc.id, {
          section: section ? section.id : "",
          quote: citation.quote || "",
        })) + '">Open cited note →</a>'
      : "";
    inspectorDom.noteLink.innerHTML = noteLink;
    inspectorDom.detail.innerHTML = rows.map(([key, value]) =>
      "<dt>" + escapeHtml(key) + "</dt><dd>" + escapeHtml(String(value)) + "</dd>"
    ).join("") + quote;

    inspectorDom.trace.innerHTML = '<p class="trace__quiet">Reading stored trace…</p>';

    inspectorDom.inspector.hidden = false;
    inspectorDom.scrim.hidden = false;
    document.body.classList.add("panel-open");
    window.requestAnimationFrame(() => {
      inspectorDom.inspector.classList.add("is-open");
      inspectorDom.scrim.classList.add("is-open");
    });
    inspectorDom.close.focus();
    fillTrace(active.turn, token);
  }

  function closeInspector() {
    if (!inspectorDom.inspector || inspectorDom.inspector.hidden) return;
    inspectorState.token += 1;
    inspectorDom.inspector.classList.remove("is-open");
    inspectorDom.scrim.classList.remove("is-open");
    document.body.classList.remove("panel-open");

    const finish = () => {
      if (!inspectorDom.inspector.classList.contains("is-open")) {
        inspectorDom.inspector.hidden = true;
        inspectorDom.scrim.hidden = true;
      }
    };
    if (prefersReducedMotion()) finish();
    else window.setTimeout(finish, 360);

    const previous = inspectorState.lastFocus;
    inspectorState.lastFocus = null;
    inspectorState.active = null;
    if (previous && typeof previous.focus === "function") {
      try {
        previous.focus({ preventScroll: true });
      } catch (error) {
        previous.focus();
      }
    }
  }

  async function copyActiveCitation() {
    if (!inspectorState.active) return;
    try {
      await copyText(citationText(inspectorState.active));
      inspectorDom.copyStatus.textContent = "Copied";
      window.setTimeout(() => { inspectorDom.copyStatus.textContent = ""; }, 2200);
    } catch (error) {
      inspectorDom.copyStatus.textContent = "Copy failed";
    }
  }

  function initInspector() {
    inspectorDom.inspector = document.getElementById("inspector");
    inspectorDom.close = document.getElementById("inspector-close");
    inspectorDom.title = document.getElementById("inspector-title");
    inspectorDom.sub = document.getElementById("inspector-sub");
    inspectorDom.detail = document.getElementById("cite-detail");
    inspectorDom.trace = document.getElementById("inspector-trace");
    inspectorDom.copy = document.getElementById("copy-citation");
    inspectorDom.copyStatus = document.getElementById("copy-status");
    inspectorDom.scrim = document.getElementById("scrim");
    inspectorDom.noteLink = document.getElementById("inspector-note-link");
    if (!inspectorDom.inspector) return;

    inspectorDom.close.addEventListener("click", closeInspector);
    inspectorDom.scrim.addEventListener("click", closeInspector);
    inspectorDom.copy.addEventListener("click", copyActiveCitation);
    inspectorDom.noteLink.addEventListener("click", (event) => {
      const link = event.target.closest("a[data-open-cited-note]");
      if (!link) return;
      event.preventDefault();
      closeInspector();
      navigate(link.getAttribute("href"));
    });

    document.addEventListener("keydown", (event) => {
      if (!inspectorDom.inspector || inspectorDom.inspector.hidden) return;
      if (event.key === "Escape") {
        event.preventDefault();
        closeInspector();
        return;
      }
      if (event.key !== "Tab") return;
      const focusable = Array.from(inspectorDom.inspector.querySelectorAll(
        'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
      )).filter((node) => !node.disabled && node.offsetParent !== null);
      if (!focusable.length) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    });
  }

  /* ------------------------------------------------------------------------
     Coverage helpers shared by both surfaces
     ------------------------------------------------------------------------ */

  function setCompanies(companies, asOf) {
    RD.state.companies = asArray(companies).filter((company) => company && company.ticker);
    RD.state.asOf = asOf || null;
  }

  function coverageStats() {
    const companies = RD.state.companies;
    const periods = companies.reduce((total, company) => total + asArray(company.periods).length, 0);
    const operational = companies.reduce((total, company) => total + asArray(company.operational_periods).length, 0);
    return {
      issuers: companies.length,
      periods: periods,
      operational: operational,
      asOf: RD.state.asOf,
    };
  }

  function coverageLabel(value) {
    if (!value) return "";
    if (typeof value === "string") return value;
    if (typeof value !== "object") return String(value);
    const pieces = [];
    if (value.financials) pieces.push("financials " + value.financials);
    if (value.operational) pieces.push("operational " + String(value.operational).replace(/_/g, " "));
    if (value.notes) pieces.push("notes " + value.notes);
    if (value.analysis) pieces.push("analysis " + value.analysis);
    if (value.expected_documents) pieces.push(asArray(value.expected_documents).length + " documents expected");
    if (value.as_of) pieces.push("as of " + formatDate(value.as_of));
    if (value.gaps && asArray(value.gaps).length) pieces.push(asArray(value.gaps).length + " gap" + (asArray(value.gaps).length === 1 ? "" : "s"));
    if (!pieces.length) {
      try {
        return Object.entries(value).slice(0, 4).map(([key, item]) =>
          humanKey(key) + ": " + (Array.isArray(item) ? item.join(", ") : String(item))
        ).join(" · ");
      } catch (error) {
        return "";
      }
    }
    return pieces.join(" · ");
  }

  function docMetaLine(doc) {
    const parts = [];
    if (doc.ticker) parts.push(doc.ticker);
    if (doc.period || doc.year) parts.push([doc.period, doc.year].filter(Boolean).join(" "));
    if (!parts.length) parts.push(humanState(doc.kind));
    return parts.join(" · ");
  }

  /* ------------------------------------------------------------------------
     Public namespace
     ------------------------------------------------------------------------ */

  RD.esc = escapeHtml;
  RD.escapeHtml = escapeHtml;
  RD.escapeAttr = escapeAttr;
  RD.asArray = asArray;
  RD.objectOr = objectOr;
  RD.pad2 = pad2;
  RD.debounce = debounce;
  RD.prefersReducedMotion = prefersReducedMotion;
  RD.isMobile = isMobile;
  RD.formatNumber = formatNumber;
  RD.formatCost = formatCost;
  RD.formatDuration = formatDuration;
  RD.formatDate = formatDate;
  RD.formatClock = formatClock;
  RD.formatBytes = formatBytes;
  RD.humanState = humanState;
  RD.humanKey = humanKey;
  RD.announce = announce;
  RD.readJson = readJson;
  RD.errorMessage = errorMessage;
  RD.safeUrl = safeUrl;
  RD.slugify = slugify;
  RD.copyText = copyText;

  RD.markdown = {
    inline: (text, options) => renderInline(String(text === undefined || text === null ? "" : text), options || {}, 0),
    render: renderMarkdown,
    renderDocument: renderDocument,
    parseFrontmatter: parseFrontmatter,
    slugify: slugify,
  };

  RD.frontmatter = parseFrontmatter;

  RD.claim = {
    normalizeType: normalizeClaimType,
    badgeLabel: badgeLabel,
    badgeDescription: badgeDescription,
    violationLabel: violationLabel,
    normalize: normalizeClaimType,
  };

  RD.citations = {
    summary: citationSummary,
    text: citationText,
    isDocument: isDocumentCitation,
    document: citationDocument,
    section: citationSection,
    from: citationFrom,
    count: countCitations,
    chip: renderChip,
  };

  RD.documents = documents;
  RD.findSection = findSection;
  RD.getDocument = getDocument;
  RD.docHref = docHref;
  RD.deskHref = deskHref;
  RD.navigate = navigate;
  RD.postAsk = postAsk;
  RD.resolveWiki = (target) => documents.resolve(target);

  RD.answer = {
    status: resultStatus,
    render: renderResult,
    renderAnswered: renderAnswered,
    renderRefused: renderRefused,
    renderUnsupported: renderUnsupported,
    renderError: renderErrorPanel,
    renderPending: renderPending,
    renderCoverage: renderCoverage,
    renderComparison: renderComparison,
    collectComparisons: collectComparisons,
    missing: renderMissing,
    verdict: verdictFooter,
    summary: resultSummary,
    announce: announceResult,
  };

  RD.renderPending = renderPending;
  RD.renderResult = renderResult;
  RD.renderCoverage = renderCoverage;
  RD.renderComparison = renderComparison;
  RD.renderMissing = renderMissing;
  RD.verdictFooter = verdictFooter;
  RD.resultSummary = resultSummary;
  RD.resultStatus = resultStatus;
  RD.announceResult = announceResult;
  RD.scopeText = scopeText;
  RD.historyFrom = historyFrom;
  RD.trimText = trimText;
  RD.countCitations = countCitations;

  RD.inspector = {
    init: initInspector,
    open: openInspector,
    close: closeInspector,
    renderTrace: renderTrace,
  };

  RD.loadTrace = loadTrace;
  RD.comparisonsFromTrace = comparisonsFromTrace;

  RD.coverage = {
    setCompanies: setCompanies,
    stats: coverageStats,
    label: coverageLabel,
    docMetaLine: docMetaLine,
  };
})(window);
