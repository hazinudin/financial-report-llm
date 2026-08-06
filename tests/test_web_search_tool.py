import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts" / "analysis_pipeline"))

import search_backend
import search_extractors
import tool_registry
import research
import research_questions
import research_plan
import opencode_client


class TestURLValidation(unittest.TestCase):
    def test_accepts_https_url(self):
        ok, err = search_backend.validate_url("https://reuters.com/article/123")
        self.assertTrue(ok, msg=err)

    def test_accepts_http_url(self):
        ok, err = search_backend.validate_url("http://bisnis.com/page")
        self.assertTrue(ok, msg=err)

    def test_rejects_file_scheme(self):
        ok, err = search_backend.validate_url("file:///etc/passwd")
        self.assertFalse(ok)
        self.assertIn("scheme", err.lower())

    def test_rejects_localhost(self):
        ok, err = search_backend.validate_url("http://localhost:8080/search")
        self.assertFalse(ok)
        self.assertIn("private", err.lower())

    def test_rejects_loopback_ip(self):
        ok, err = search_backend.validate_url("http://127.0.0.1/admin")
        self.assertFalse(ok)
        self.assertIn("private", err.lower())

    def test_rejects_private_network(self):
        for host in ("192.168.1.1", "10.0.0.5", "172.16.0.1"):
            with self.subTest(host=host):
                ok, err = search_backend.validate_url(f"https://{host}/x")
                self.assertFalse(ok, msg=f"{host}: {err}")

    def test_rejects_empty_url(self):
        ok, err = search_backend.validate_url("")
        self.assertFalse(ok)

    def test_rejects_no_hostname(self):
        ok, err = search_backend.validate_url("https://")
        self.assertFalse(ok)


class TestSourceClassification(unittest.TestCase):
    def test_news_domain(self):
        self.assertEqual(search_backend.classify_source("reuters.com"), "news")
        self.assertEqual(search_backend.classify_source("bloomberg.com"), "news")
        self.assertEqual(search_backend.classify_source("cnbcindonesia.com"), "news")

    def test_government_domain(self):
        self.assertEqual(search_backend.classify_source("esdm.go.id"), "government")
        self.assertEqual(search_backend.classify_source("bi.go.id"), "government")
        self.assertEqual(search_backend.classify_source("kemenkeu.go.id"), "government")

    def test_filing_domain(self):
        self.assertEqual(search_backend.classify_source("idx.co.id"), "filing")

    def test_unknown_domain(self):
        self.assertEqual(search_backend.classify_source("random-blog.xyz"), "unknown")

    def test_subdomain_matches(self):
        self.assertEqual(search_backend.classify_source("sub.reuters.com"), "news")
        self.assertEqual(
            search_backend.classify_source("testing.esdm.go.id"), "government"
        )


class TestCacheKeys(unittest.TestCase):
    def test_same_inputs_same_key(self):
        k1 = search_backend._make_cache_key("nickel price", "news", "month", "5", "searxng", "1")
        k2 = search_backend._make_cache_key("nickel price", "news", "month", "5", "searxng", "1")
        self.assertEqual(k1, k2)

    def test_different_query_different_key(self):
        k1 = search_backend._make_cache_key("nickel price", "news", "month", "5", "searxng", "1")
        k2 = search_backend._make_cache_key("coal price", "news", "month", "5", "searxng", "1")
        self.assertNotEqual(k1, k2)

    def test_different_version_different_key(self):
        k1 = search_backend._make_cache_key("nickel price", "news", "month", "5", "searxng", "1")
        k2 = search_backend._make_cache_key("nickel price", "news", "month", "5", "searxng", "2")
        self.assertNotEqual(k1, k2)

    def test_query_normalization_stable(self):
        q1 = search_backend._normalize_query("  Nickel   PRICE 2026  ")
        self.assertEqual(q1, "nickel price 2026")


class TestCacheReadWrite(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.search_dir = Path(self.tmp.name) / "search"
        self.fetch_dir = Path(self.tmp.name) / "fetch"
        self.search_dir.mkdir(parents=True)
        self.fetch_dir.mkdir(parents=True)
        self._orig_search = search_backend.SEARCH_CACHE_DIR
        self._orig_fetch = search_backend.FETCH_CACHE_DIR
        search_backend.SEARCH_CACHE_DIR = self.search_dir
        search_backend.FETCH_CACHE_DIR = self.fetch_dir

    def tearDown(self):
        search_backend.SEARCH_CACHE_DIR = self._orig_search
        search_backend.FETCH_CACHE_DIR = self._orig_fetch
        self.tmp.cleanup()

    def test_write_and_read_cache(self):
        key = "abc123"
        data = {"status": "ok", "results": [{"title": "Test"}]}
        path = search_backend._search_cache_path(key)
        search_backend._write_cache(path, data)
        cached = search_backend._read_cache(path)
        self.assertIsNotNone(cached)
        self.assertEqual(cached["status"], "ok")
        self.assertEqual(cached["results"][0]["title"], "Test")

    def test_read_missing_cache_returns_none(self):
        path = search_backend._search_cache_path("nonexistent")
        self.assertIsNone(search_backend._read_cache(path))

    def test_cache_survives_roundtrip(self):
        data = {
            "status": "ok",
            "query": "test query",
            "cache_hit": False,
            "results": [
                {
                    "result_id": "sha256:abc",
                    "title": "Article",
                    "url": "https://example.com",
                    "snippet": "Content here",
                    "published_date": "2026-01-15",
                    "source_domain": "example.com",
                    "source_type": "news",
                    "fetchable": True,
                    "authoritative": False,
                }
            ],
            "errors": [],
            "usage": {"query_count": 1},
        }
        key = "roundtrip"
        path = search_backend._search_cache_path(key)
        search_backend._write_cache(path, data)
        loaded = search_backend._read_cache(path)
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded["query"], "test query")
        self.assertEqual(len(loaded["results"]), 1)
        self.assertEqual(loaded["results"][0]["title"], "Article")


class TestHTMLExtraction(unittest.TestCase):
    def test_simple_page(self):
        html = (
            "<html><head><title>Test Page</title></head>"
            "<body><h1>Hello</h1><p>Paragraph one.</p>"
            "<p>Paragraph two.</p></body></html>"
        )
        r = search_extractors.extract_html(html)
        self.assertEqual(r["status"], "ok")
        self.assertEqual(r["title"], "Test Page")
        self.assertIn("Paragraph one.", r["content"])
        self.assertIn("Paragraph two.", r["content"])

    def test_js_shell_detected(self):
        html = "<noscript>Please enable JavaScript to view this page.</noscript>"
        r = search_extractors.extract_html(html)
        self.assertEqual(r["status"], "degraded")
        self.assertIn("JavaScript-shell", r.get("error", ""))

    def test_script_content_excluded(self):
        html = (
            "<html><body>"
            "<script>var x = 1;</script>"
            "<p>Visible text.</p>"
            "</body></html>"
        )
        r = search_extractors.extract_html(html)
        self.assertNotIn("var x = 1", r["content"])
        self.assertIn("Visible text.", r["content"])

    def test_title_extraction(self):
        html = "<html><head><title>My Title</title></head><body><p>Body.</p></body></html>"
        r = search_extractors.extract_html(html)
        self.assertEqual(r["title"], "My Title")


class TestToolRegistry(unittest.TestCase):
    def test_schemas_have_required_fields(self):
        for schema in tool_registry.TOOL_SCHEMAS:
            self.assertIn("type", schema)
            self.assertIn("function", schema)
            self.assertIn("name", schema["function"])
            self.assertIn("parameters", schema["function"])

    def test_unknown_tool_rejected(self):
        budget = tool_registry.BudgetState()
        result = tool_registry.execute_tool("nonexistent_tool", {}, budget)
        self.assertEqual(result["status"], "error")
        self.assertIn("unknown", result.get("error", ""))

    def test_search_budget_exhausted(self):
        budget = tool_registry.BudgetState(max_searches=0)
        result = tool_registry.execute_tool(
            "web_search", {"query": "test"}, budget
        )
        self.assertEqual(result["status"], "degraded")
        self.assertIn("budget", result.get("error", ""))

    def test_fetch_budget_exhausted(self):
        budget = tool_registry.BudgetState(max_fetches=0)
        result = tool_registry.execute_tool(
            "fetch_web_page", {"url": "https://reuters.com/article"}, budget
        )
        self.assertEqual(result["status"], "degraded")
        self.assertIn("budget", result.get("error", ""))

    def test_fetch_blocked_for_unknown_domain(self):
        budget = tool_registry.BudgetState(max_fetches=5)
        result = tool_registry.execute_tool(
            "fetch_web_page", {"url": "https://evil.phishing.site/page"}, budget
        )
        self.assertEqual(result["status"], "blocked")
        self.assertIn("domain", result.get("error", ""))
        self.assertEqual(budget.fetch_count, 0)  # blocked fetches don't count

    def test_budget_increments_on_valid_call(self):
        budget = tool_registry.BudgetState(max_searches=5, max_fetches=5)
        self.assertEqual(budget.search_count, 0)
        tool_registry.execute_tool("web_search", {"query": "test"}, budget)
        self.assertEqual(budget.search_count, 1)


class TestDomainAllowList(unittest.TestCase):
    def test_allowed_domain_matches_exact(self):
        self.assertTrue(
            tool_registry._is_domain_allowed("https://reuters.com/article")
        )

    def test_allowed_subdomain_matches(self):
        self.assertTrue(
            tool_registry._is_domain_allowed("https://asia.reuters.com/article")
        )

    def test_unknown_domain_rejected(self):
        self.assertFalse(
            tool_registry._is_domain_allowed("https://random-site.xyz/page")
        )

    def test_empty_allow_list_blocks_all(self):
        with patch.object(tool_registry, "_research_config", return_value={}):
            self.assertFalse(
                tool_registry._is_domain_allowed("https://reuters.com/article")
            )


class TestJSShellDetection(unittest.TestCase):
    def test_js_shell_with_marker(self):
        self.assertTrue(
            search_extractors._is_js_shell(
                "<html><body>Please enable JavaScript to continue.</body></html>"
            )
        )

    def test_normal_page_not_js_shell(self):
        self.assertFalse(
            search_extractors._is_js_shell(
                "<html><body><p>This is a long paragraph with substantial "
                "content that describes something meaningful about the "
                "company's financial performance in the latest quarter.</p></body></html>"
            )
        )

    def test_short_page_without_marker_not_js_shell(self):
        self.assertFalse(
            search_extractors._is_js_shell(
                "<html><body><p>Short.</p></body></html>"
            )
        )


class TestSearchBackendSchemas(unittest.TestCase):
    def test_search_response_defaults(self):
        resp = search_backend.SearchResponse(
            status="ok", query="test", cache_hit=True
        )
        self.assertEqual(resp.status, "ok")
        self.assertEqual(resp.cache_hit, True)
        self.assertEqual(resp.results, [])

    def test_page_response_defaults(self):
        resp = search_backend.PageResponse(
            status="blocked", url="https://evil.com", error="blocked by policy"
        )
        self.assertEqual(resp.status, "blocked")
        self.assertEqual(resp.content, "")
        self.assertEqual(resp.fetch_attempted, False)


class TestResearchProvenance(unittest.TestCase):
    def test_unverified_sources_are_removed(self):
        brief = {
            "findings": [{
                "event": "margin changed",
                "confidence": "medium",
                "sources": [{"title": "Invented", "url": "https://fake.test/story"}],
            }],
        }
        audit = research._validate_brief_sources(brief, [], no_tool_calls=True)
        self.assertEqual(brief["findings"][0]["sources"], [])
        self.assertEqual(brief["findings"][0]["confidence"], "low")
        self.assertEqual(audit["unverified_sources"][0]["url"], "https://fake.test/story")
        self.assertFalse(audit["passed"])

    def test_search_result_url_is_valid_provenance(self):
        brief = {
            "findings": [{
                "event": "margin changed",
                "confidence": "medium",
                "sources": [{"title": "Real", "url": "https://Reuters.com/story/"}],
            }],
        }
        trace = [{
            "tool_name": "web_search",
            "result": {"results": [{"url": "https://reuters.com/story"}]},
        }]
        audit = research._validate_brief_sources(brief, trace, no_tool_calls=False)
        self.assertEqual(len(brief["findings"][0]["sources"]), 1)
        self.assertEqual(audit["verified_source_count"], 1)
        self.assertTrue(audit["passed"])


class TestResearchQuestions(unittest.TestCase):
    def test_mhki_identity_rejects_mahaka_alias(self):
        identity = research_questions.resolve_identity("mhki")
        self.assertEqual(identity.legal_name, "PT Multi Hanna Kreasindo Tbk")
        self.assertIn("ABBA", identity.forbidden_identities)
        self.assertIn("Mahaka Media", identity.forbidden_identities)

    def test_questions_include_canonical_identity(self):
        identity, questions = research_questions.build_questions(
            "MHKI", 2026, "Q2",
            "- Income-statement line 'interest_income' increased 146% vs prior period",
        )
        self.assertEqual(identity.ticker, "MHKI")
        self.assertEqual(len(questions), 1)
        self.assertIn("Multi Hanna", questions[0].question)
        self.assertIn("MHKI", questions[0].search_query)
        self.assertNotIn("ABBA", questions[0].search_query)

    def test_question_generation_is_bounded_and_deduplicated(self):
        events = "\n".join([
            "- trade_receivables_net increased 216%",
            "- trade_receivables_net increased 216%",
            "- prepaid_taxes increased 59%",
        ])
        _, questions = research_questions.build_questions(
            "MHKI", 2026, "Q2", events, max_questions=1,
        )
        self.assertEqual(len(questions), 1)
        self.assertEqual(questions[0].question_id, "mhki-01")


class TestResearchPlan(unittest.TestCase):
    _SAMPLE = (
        "```yaml\n"
        "analytical_framing: |\n"
        "  Q2 = H1 - Q1. Ethanol +13% QoQ.\n"
        "themes:\n"
        "  - name: \"Ethanol recovery\"\n"
        "    priority: high\n"
        "    rationale: \"core segment\"\n"
        "    questions:\n"
        "      - id: \"A1\"\n"
        "        question: \"Did molasses harvest lift Q2 volume?\"\n"
        "        rationale: \"explains +13%\"\n"
        "        search_query: \"MHKI Q2 2026 ethanol\"\n"
        "hypotheses:\n"
        "  - \"Seasonal pull-forward\"\n"
        "priority_themes:\n"
        "  - \"Ethanol recovery\"\n"
        "```\n"
    )

    def setUp(self):
        self.identity = research_questions.resolve_identity("MHKI")

    def test_parse_plan_strips_fences_and_fields(self):
        plan = research_plan.parse_plan(
            self._SAMPLE, "MHKI", 2026, "Q2", self.identity,
        )
        self.assertIsNone(plan.error)
        self.assertEqual(len(plan.questions), 1)
        self.assertEqual(plan.questions[0].id, "A1")
        self.assertEqual(plan.questions[0].theme, "Ethanol recovery")
        self.assertIn("molasses", plan.questions[0].question.lower())
        self.assertEqual(len(plan.themes), 1)
        self.assertEqual(plan.themes[0]["priority"], "high")
        self.assertEqual(plan.hypotheses, ["Seasonal pull-forward"])
        self.assertEqual(plan.priority_themes, ["Ethanol recovery"])
        self.assertIn("Q2 = H1", plan.analytical_framing)

    def test_parse_plan_auto_assigns_id_when_missing(self):
        raw = (
            "themes:\n"
            "  - name: \"Theme A\"\n"
            "    questions:\n"
            "      - {question: \"q1\"}\n"
            "      - {question: \"q2\"}\n"
        )
        plan = research_plan.parse_plan(raw, "DKFT", 2026, "Q1", self.identity)
        self.assertEqual([q.id for q in plan.questions], ["A1", "A2"])

    def test_parse_plan_handles_malformed_yaml(self):
        plan = research_plan.parse_plan("just a plain prose string, not yaml", "DKFT", 2026, "Q1", self.identity)
        self.assertIsNotNone(plan.error)
        self.assertEqual(plan.questions, [])

    def test_parse_plan_empty_input(self):
        plan = research_plan.parse_plan("", "DKFT", 2026, "Q1", self.identity)
        self.assertIsNotNone(plan.error)

    def test_parse_plan_strips_prose_preamble(self):
        raw = (
            "Here is the research plan you requested.\n\n"
            "I will first derive Q2 = H1 - Q1.\n\n"
            "```yaml\n"
            "analytical_framing: |\n"
            "  Q2 derived.\n"
            "themes:\n"
            "  - name: \"Theme A\"\n"
            "    questions:\n"
            "      - id: \"A1\"\n"
            "        question: \"why?\"\n"
            "```\n"
        )
        plan = research_plan.parse_plan(raw, "MHKI", 2026, "Q2", self.identity)
        self.assertIsNone(plan.error)
        self.assertEqual(len(plan.questions), 1)

    def test_parse_plan_uses_final_yaml_after_schema_discussion(self):
        raw = (
            "Structure:\n"
            "analytical_framing: |\n"
            "  <text>\n"
            "themes:\n"
            "  - name: example\n"
            "Let's draft the actual plan.\n"
            "analytical_framing: |\n"
            "  Q2 = H1 - Q1.\n"
            "themes:\n"
            "  - name: \"Actual theme\"\n"
            "    priority: high\n"
            "    questions:\n"
            "      - id: \"A1\"\n"
            "        question: \"What changed?\"\n"
        )
        plan = research_plan.parse_plan(raw, "MHKI", 2026, "Q2", self.identity)
        self.assertIsNone(plan.error)
        self.assertEqual(plan.themes[0]["name"], "Actual theme")
        self.assertEqual(len(plan.questions), 1)

    def test_parse_plan_salvages_colon_in_plain_scalar(self):
        # Model wrote a colon inside a plain scalar — the exact failure
        # observed on the MHKI Q2 run ("could mean:").
        raw = (
            'analytical_framing: The "increased 146% vs prior period" for Q2 2026 could mean: a margin jump.\n'
            'themes:\n'
            '  - name: "Theme A"\n'
            '    questions:\n'
            '      - id: "A1"\n'
            '        question: "why did it jump?"\n'
        )
        plan = research_plan.parse_plan(raw, "MHKI", 2026, "Q2", self.identity)
        self.assertIsNone(plan.error)
        self.assertIn("could mean", plan.analytical_framing)
        self.assertEqual(len(plan.questions), 1)

    def test_fallback_plan_shapes_deterministic_questions(self):
        identity, qs = research_questions.build_questions(
            "MHKI", 2026, "Q2",
            "- trade_receivables_net increased 216%",
        )
        plan = research_plan._fallback_plan(
            "MHKI", 2026, "Q2", identity, qs, reason="planner unavailable",
        )
        self.assertTrue(plan.fallback)
        self.assertIsNotNone(plan.error)
        self.assertEqual(len(plan.questions), len(qs))
        # carries the deterministic question_id as the plan question id
        self.assertEqual(plan.questions[0].id, qs[0].question_id)
        self.assertEqual(plan.questions[0].theme, "Detected events")

    def test_plan_questions_for_prompt_includes_framing_theme_and_ids(self):
        plan = research_plan.parse_plan(
            self._SAMPLE, "MHKI", 2026, "Q2", self.identity,
        )
        out = research_plan.plan_questions_for_prompt(plan)
        self.assertIn("Analytical framing", out)
        self.assertIn("[A1]", out)
        self.assertIn("Ethanol recovery", out)
        self.assertIn("Hypotheses to test", out)

    def test_plan_as_markdown_includes_identity_and_themes(self):
        plan = research_plan.parse_plan(
            self._SAMPLE, "MHKI", 2026, "Q2", self.identity,
        )
        md = research_plan.plan_as_markdown(plan)
        self.assertIn("PT Multi Hanna Kreasi Indonesia Tbk (MHKI)", md)
        self.assertIn("### Ethanol recovery (priority: high)", md)
        self.assertIn("Seasonal pull-forward", md)
        # forbidden identities surfaced for the human reader
        self.assertIn("PT Mahaka Media Tbk", md)

    def test_plan_as_dicts_round_trips_questions(self):
        plan = research_plan.parse_plan(
            self._SAMPLE, "MHKI", 2026, "Q2", self.identity,
        )
        d = research_plan.plan_as_dicts(plan)
        self.assertEqual(len(d["questions"]), 1)
        self.assertEqual(d["questions"][0]["search_query"], "MHKI Q2 2026 ethanol")
        self.assertFalse(d["fallback"])

    def test_formulate_plan_falls_back_when_no_model(self):
        with patch.object(
            research_plan.opencode_client, "parse_model_settings",
            return_value={"model": None, "temperature": 0.0, "max_tokens": 4096, "reasoning_effort": None},
        ):
            identity, qs = research_questions.build_questions(
                "MHKI", 2026, "Q2", "- prepaid_taxes increased 59%",
            )
            plan = research_plan.formulate_plan(
                "MHKI", 2026, "Q2", "- prepaid_taxes increased 59%",
                identity=identity, deterministic_fallback=qs,
            )
        self.assertTrue(plan.fallback)
        self.assertNotEqual(plan.questions, [])

    def test_formulate_plan_falls_back_on_call_failure(self):
        with patch.object(
            research_plan.opencode_client, "parse_model_settings",
            return_value={"model": "kimi-k2.6", "temperature": 0.0, "max_tokens": 4096, "reasoning_effort": None},
        ), patch.object(
            research_plan.opencode_client, "chat",
            side_effect=RuntimeError("boom"),
        ):
            identity, qs = research_questions.build_questions(
                "MHKI", 2026, "Q2", "- prepaid_taxes increased 59%",
            )
            plan = research_plan.formulate_plan(
                "MHKI", 2026, "Q2", "- prepaid_taxes increased 59%",
                identity=identity, deterministic_fallback=qs,
            )
        self.assertTrue(plan.fallback)
        self.assertIn("boom", (plan.error or ""))

    def test_formulate_plan_falls_back_when_planner_emits_no_questions(self):
        with patch.object(
            research_plan.opencode_client, "parse_model_settings",
            return_value={"model": "kimi-k2.6", "temperature": 0.0, "max_tokens": 4096, "reasoning_effort": None},
        ), patch.object(
            research_plan.opencode_client, "chat",
            return_value={"content": "themes: []\nhypotheses: []\n"},
        ):
            identity, qs = research_questions.build_questions(
                "MHKI", 2026, "Q2", "- prepaid_taxes increased 59%",
            )
            plan = research_plan.formulate_plan(
                "MHKI", 2026, "Q2", "- prepaid_taxes increased 59%",
                identity=identity, deterministic_fallback=qs,
            )
        self.assertTrue(plan.fallback)

    def test_formulate_plan_reports_truncation_and_persists_call_metadata(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(
            research_plan, "RESEARCH_DIR", Path(directory),
        ), patch.object(
            research_plan, "build_planner_context", return_value="context",
        ), patch.object(
            research_plan.opencode_client, "parse_model_settings",
            return_value={
                "model": "kimi-k2.6", "temperature": 0.0,
                "max_tokens": 12288, "reasoning_effort": "low",
            },
        ), patch.object(
            research_plan.opencode_client, "chat",
            return_value={
                "content": "The plan is still being derived...",
                "usage": {"prompt_tokens": 100, "completion_tokens": 12288},
                "finish_reason": "length",
            },
        ):
            identity, qs = research_questions.build_questions(
                "MHKI", 2026, "Q2", "- revenue decreased 19%",
            )
            plan = research_plan.formulate_plan(
                "MHKI", 2026, "Q2", "- revenue decreased 19%",
                identity=identity, deterministic_fallback=qs,
            )

            self.assertTrue(plan.fallback)
            self.assertIn("truncated at max_tokens", plan.error or "")
            raw = (Path(directory) / "MHKI_Q2_2026.research_plan_raw.md").read_text()
            self.assertIn("finish_reason: length", raw)
            self.assertIn("completion_tokens: 12288", raw)


class TestOpenCodeToolLoop(unittest.TestCase):
    def test_exhausted_rounds_use_text_only_final_synthesis(self):
        calls = []
        responses = iter([
            {
                "choices": [{
                    "message": {
                        "content": "Searching...",
                        "tool_calls": [{
                            "id": "call-1",
                            "function": {
                                "name": "web_search",
                                "arguments": '{"query":"MHKI"}',
                            },
                        }]
                    },
                    "finish_reason": "tool_calls",
                }],
                "usage": {"prompt_tokens": 10, "completion_tokens": 5},
            },
            {
                "choices": [{
                    "message": {"content": "findings:\n  - event: test"},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 20, "completion_tokens": 8},
            },
        ])

        def fake_model_call(**kwargs):
            calls.append(kwargs)
            return next(responses)

        with patch.object(opencode_client, "_model_call", side_effect=fake_model_call):
            result = opencode_client.chat_with_tools(
                model="hy3",
                messages=[{"role": "user", "content": "research"}],
                tools=[{"type": "function", "function": {"name": "web_search"}}],
                tool_executor=lambda _name, _args: {"status": "ok"},
                max_tool_rounds=1,
                max_tokens=8192,
            )

        self.assertEqual(result["content"], "findings:\n  - event: test")
        self.assertEqual(result["finish_reason"], "stop")
        self.assertIsNotNone(calls[0]["tools"])
        self.assertIsNone(calls[1]["tools"])
        self.assertIsNone(result["error"])


if __name__ == "__main__":
    unittest.main()
