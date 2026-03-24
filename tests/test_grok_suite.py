from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
PLUGIN_PATH = REPO_ROOT / "main.py"


def load_grok_plugin_module():
    module_name = "tests.dynamic_astrbot_plugin_grok_suite"
    spec = importlib.util.spec_from_file_location(module_name, PLUGIN_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


class FakeResponse:
    def __init__(self, *, status: int = 200, body: bytes = b"") -> None:
        self.status = status
        self._body = body
        self.content = SimpleNamespace()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def read(self) -> bytes:
        return self._body

    async def text(self) -> str:
        return self._body.decode("utf-8", errors="ignore")


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self._responses = list(responses)
        self.calls: list[dict] = []

    def post(self, url, headers=None, json=None, timeout=None):
        self.calls.append(
            {
                "url": url,
                "headers": headers,
                "json": json,
                "timeout": timeout,
            }
        )
        if not self._responses:
            raise AssertionError("No fake responses left for session.post")
        return self._responses.pop(0)


@pytest.mark.asyncio
async def test_edit_image_falls_back_to_non_stream_when_stream_media_parse_fails(
    monkeypatch,
    tmp_path: Path,
):
    plugin_module = load_grok_plugin_module()
    monkeypatch.setattr(
        plugin_module.StarTools,
        "get_data_dir",
        lambda *args, **kwargs: str(tmp_path),
    )

    plugin = plugin_module.GrokPlugin(
        context=MagicMock(),
        config={
            "grok_api_key": "test-key",
            "grok_api_url": "https://api.x.ai",
            "stream_enabled": True,
        },
    )
    plugin.IMAGE_RESPONSE_FORMAT_CANDIDATES = ("url",)
    plugin.MAX_REQUEST_RETRIES = 1

    async def fake_resolve_model(**kwargs):
        return "grok-imagine-1.0-edit"

    async def fake_parse_media_response(resp, media_type="image"):
        return None, None, "未能从响应中提取媒体内容"

    success_payload = json.dumps(
        {"data": [{"url": "https://example.com/generated.png"}]}
    ).encode("utf-8")
    fake_session = FakeSession(
        [
            FakeResponse(status=200),
            FakeResponse(status=200, body=success_payload),
        ]
    )

    async def fake_ensure_session():
        return fake_session

    monkeypatch.setattr(plugin, "_resolve_model", fake_resolve_model)
    monkeypatch.setattr(plugin, "_parse_media_response", fake_parse_media_response)
    monkeypatch.setattr(plugin, "_ensure_session", fake_ensure_session)

    results, error = await plugin._edit_image_via_chat(
        prompt="turn it into a watercolor painting",
        image_bytes=b"\x89PNG\r\n\x1a\nfake",
        stream_preference=True,
    )

    assert error is None
    assert results == [("https://example.com/generated.png", None)]
    assert [call["json"]["stream"] for call in fake_session.calls] == [True, False]
