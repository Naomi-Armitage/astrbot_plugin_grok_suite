import asyncio
import base64
import hashlib
import io
import json
import mimetypes
import os
import time
import re
import uuid
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any
from urllib.parse import urlparse, unquote
Image = None
try:
    from PIL import Image
except ImportError:
    pass

import aiohttp
import aiofiles
from astrbot.api import logger
from astrbot.api.event import filter
from astrbot.api.star import Context, Star, StarTools
from astrbot.core import AstrBotConfig
import astrbot.api.message_components as Comp
from astrbot.core.platform.astr_message_event import AstrMessageEvent

try:
    from astrbot.core.provider.entities import ProviderRequest
    from astrbot.core.provider.func_tool_manager import FunctionToolManager
except ImportError:
    ProviderRequest = Any
    FunctionToolManager = None

try:
    from astrbot.core.utils.quoted_message_parser import (
        extract_quoted_message_images as astrbot_extract_quoted_message_images,
    )
except Exception:
    try:
        from astrbot.core.utils.quoted_message.extractor import (
            extract_quoted_message_images as astrbot_extract_quoted_message_images,
        )
    except Exception:
        astrbot_extract_quoted_message_images = None



class GrokPlugin(Star):
    """Grok 多媒体与联网搜索插件 - 支持生图、生视频、联网搜索"""

    DEFAULT_TEXT_IMAGE_SIZE = "720x1280"  # 9:16 竖屏
    DEFAULT_VIDEO_SIZE = "1792x1024"      # 3:2 横构图
    DEFAULT_VIDEO_LENGTH_SECONDS = 6
    SUPPORTED_VIDEO_LENGTH_SECONDS = tuple(range(6, 31))
    VIDEO_RESOLUTION_NAME = "720p"
    SUPPORTED_IMAGE_SIZES = (
        "1024x1024",
        "1024x1792",
        "1280x720",
        "1792x1024",
        "720x1280",
    )
    SIZE_TO_ASPECT_RATIO = {
        "1280x720": "16:9",
        "720x1280": "9:16",
        "1792x1024": "3:2",
        "1024x1792": "2:3",
        "1024x1024": "1:1",
    }
    # 反向映射：比例 → 像素尺寸（用于用户输入比例格式时的转换）
    ASPECT_RATIO_TO_SIZE = {
        "16:9": "1280x720",
        "9:16": "720x1280",
        "3:2": "1792x1024",
        "2:3": "1024x1792",
        "1:1": "1024x1024",
    }
    RATIO_ALIASES = {
        "16：9": "16:9",
        "9：16": "9:16",
        "3：2": "3:2",
        "2：3": "2:3",
        "1：1": "1:1",
        "横屏": "16:9",
        "横版": "16:9",
        "横构图": "16:9",
        "竖屏": "9:16",
        "竖版": "9:16",
        "竖构图": "9:16",
        "方图": "1:1",
        "方形": "1:1",
        "正方形": "1:1",
    }
    DEFAULT_SEARCH_MODEL = "grok-4-fast"
    DEFAULT_SEARCH_TIMEOUT = 60.0
    DEFAULT_SEARCH_THINKING_BUDGET = 32000

    MAX_IMAGE_COUNT = 10
    MAX_STREAM_LINES = 10000
    MAX_RESPONSE_BYTES = 50 * 1024 * 1024
    MIN_BASE64_LENGTH = 100
    IMAGE_TIMEOUT = 120
    VIDEO_TIMEOUT = 300
    MAX_PROMPT_LENGTH = 4000
    MAX_REQUEST_RETRIES = 3
    RETRYABLE_HTTP_STATUS_CODES = {408, 409, 425, 429, 500, 502, 503, 504}
    MODEL_CACHE_TTL_SECONDS = 300
    MODEL_PROBE_TIMEOUT = 15
    IMAGE_RESPONSE_FORMAT_CANDIDATES = ("url", "b64_json", "base64", None)
    GENERATED_IMAGE_URL_INDEX_LIMIT = 512
    RECENT_SESSION_IMAGE_TTL_SECONDS = 3600
    RECENT_SESSION_IMAGE_LIMIT = 12

    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.conf = config
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_lock = asyncio.Lock()
        self._models_cache: Dict[str, Any] = {"expires_at": 0.0, "models": set()}
        self._models_cache_lock = asyncio.Lock()
        self.plugin_data_dir = StarTools.get_data_dir("astrbot_plugin_grok_suite")
        self.temp_dir = Path(self.plugin_data_dir) / "temp"
        self.temp_image_dir = self.temp_dir / "images"
        self.temp_video_dir = self.temp_dir / "videos"
        self.cache_image_dir = Path(self.plugin_data_dir) / "cache_images"
        self.image_dir = Path(self.plugin_data_dir) / "images"
        self.video_dir = Path(self.plugin_data_dir) / "videos"
        self.generated_image_index_path = Path(self.plugin_data_dir) / "generated_image_urls.json"
        self.temp_dir.mkdir(exist_ok=True, parents=True)
        self.temp_image_dir.mkdir(exist_ok=True, parents=True)
        self.temp_video_dir.mkdir(exist_ok=True, parents=True)
        self.cache_image_dir.mkdir(exist_ok=True, parents=True)
        self.image_dir.mkdir(exist_ok=True, parents=True)
        self.video_dir.mkdir(exist_ok=True, parents=True)
        self._generated_image_url_index = self._load_generated_image_url_index()
        self._recent_session_images: Dict[str, List[Dict[str, Any]]] = {}

    async def initialize(self):
        if Image is None:
            logger.warning("Pillow 未安装，部分功能受限")
        async with self._session_lock:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession()
        self._cleanup_directory_to_limit(
            self.cache_image_dir,
            self._get_media_file_limit("image", persistent=True),
        )
        self._cleanup_directory_to_limit(
            self.image_dir,
            self._get_media_file_limit("image", persistent=True),
        )
        self._cleanup_directory_to_limit(
            self.video_dir,
            self._get_media_file_limit("video", persistent=True),
        )
        self._cleanup_directory_to_limit(
            self.temp_image_dir,
            self._get_media_file_limit("image", persistent=False),
        )
        self._cleanup_directory_to_limit(
            self.temp_video_dir,
            self._get_media_file_limit("video", persistent=False),
        )
        logger.info("Grok 多媒体生成插件初始化完成")

    async def terminate(self):
        async with self._session_lock:
            if self._session and not self._session.closed:
                try:
                    await self._session.close()
                except Exception as e:
                    logger.warning(f"关闭 session 时出错: {e}")
            self._session = None
        logger.info("Grok 多媒体生成插件已终止")

    # ==================== 工具方法 ====================

    @staticmethod
    def _strip_markdown(text: str) -> str:
        """移除文本中的 Markdown 格式，但保留排版结构（换行、段落、列表）"""
        if not text:
            return ""

        # 移除代码块标记，但保留内容和内部换行
        text = re.sub(r'```(?:\w+)?\n?([\s\S]*?)```', r'\1', text)

        # 移除行内代码标记
        text = re.sub(r'`([^`]+)`', r'\1', text)

        # 移除粗体标记
        text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)
        text = re.sub(r'__([^_]+)__', r'\1', text)

        # 移除斜体标记
        text = re.sub(r'\*([^*]+)\*', r'\1', text)
        text = re.sub(r'_([^_]+)_', r'\1', text)

        # 移除标题符号，保留标题文本和换行
        text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)

        # 转换链接格式：[文本](url) → 文本: url
        text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', r'\1: \2', text)

        # 移除图片标记，保留 alt 文本
        text = re.sub(r'!\[([^\]]*)\]\([^)]+\)', r'\1', text)

        # 移除水平线（整行删除）
        text = re.sub(r'^[-*_]{3,}\s*$\n?', '', text, flags=re.MULTILINE)

        # 移除引用符号，保留引用内容
        text = re.sub(r'^>\s+', '', text, flags=re.MULTILINE)

        # 移除多余的连续空行（保留最多一个空行）
        text = re.sub(r'\n{3,}', '\n\n', text)

        return text.strip()

    ERROR_TRANSLATIONS = {
        "Session is closed": "会话已关闭，请重试",
        "Connection reset by peer": "连接被重置，请重试",
        "Connection refused": "连接被拒绝，请检查API地址",
        "Timeout": "请求超时，请重试",
        "TimeoutError": "请求超时，请重试",
        "Name or service not known": "无法解析API地址，请检查网络",
        "No route to host": "无法连接到服务器，请检查网络",
        "Network is unreachable": "网络不可达，请检查网络连接",
        "SSL": "SSL证书错误，请检查API地址",
        "Certificate": "证书验证失败",
        "Unauthorized": "API密钥无效或已过期",
        "Forbidden": "访问被拒绝，请检查权限",
        "Not Found": "API接口不存在，请检查配置",
        "Too Many Requests": "请求过于频繁，请稍后重试",
        "Rate limit": "已达到速率限制，请稍后重试",
        "Internal Server Error": "服务器内部错误，请稍后重试",
        "Bad Gateway": "网关错误，请稍后重试",
        "Service Unavailable": "服务暂时不可用，请稍后重试",
        "Gateway Timeout": "网关超时，请稍后重试",
        "Invalid API Key": "API密钥无效",
        "Insufficient quota": "API额度不足",
        "Model not found": "模型不存在，请检查配置",
        "Content policy": "内容违反使用政策",
        "Safety system": "触发安全系统限制",
    }

    def _translate_error(self, error: str) -> str:
        """将英文错误消息翻译为中文"""
        if not error:
            return "未知错误"

        raw_error = str(error).strip()
        if not raw_error:
            return "未知错误"

        # 已经是中文，直接透传，避免二次翻译后信息丢失
        if any("\u4e00" <= c <= "\u9fff" for c in raw_error):
            return raw_error

        error_lower = raw_error.lower()

        # 检查是否匹配已知错误模式
        for en_pattern, zh_msg in self.ERROR_TRANSLATIONS.items():
            if en_pattern.lower() in error_lower:
                return zh_msg

        if "invalid_size" in error_lower or "size must be" in error_lower:
            return f"尺寸参数不合法: {raw_error}"

        if "invalid_resolution" in error_lower or "resolution_name" in error_lower:
            return f"视频分辨率参数不合法: {raw_error}"

        # 处理 HTTP 状态码
        if "状态码: 401" in raw_error or "status: 401" in error_lower:
            return "API密钥无效或已过期"
        if "状态码: 403" in raw_error or "status: 403" in error_lower:
            return "访问被拒绝"
        if "状态码: 404" in raw_error or "status: 404" in error_lower:
            return "API接口不存在"
        if "状态码: 429" in raw_error or "status: 429" in error_lower:
            return "请求过于频繁，请稍后重试"
        if "状态码: 5" in raw_error or "status: 5" in error_lower:
            return "服务器错误，请稍后重试"

        # 处理 Errno 错误
        if "errno" in error_lower:
            if "104" in raw_error:
                return "连接被重置，请重试"
            if "111" in raw_error:
                return "连接被拒绝，请检查API地址"
            if "110" in raw_error:
                return "连接超时，请重试"
            if "113" in raw_error:
                return "无法连接到服务器"

        # 提取末尾更有价值的片段
        if ":" in raw_error:
            parts = raw_error.split(":")
            for part in reversed(parts):
                part = part.strip()
                if part and not part.startswith("[") and len(part) > 3:
                    return part[:200]

        return raw_error[:200]

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """确保 session 有效（线程安全）"""
        async with self._session_lock:
            if not self._session or self._session.closed:
                self._session = aiohttp.ClientSession()
            return self._session

    def _parse_image_api_response(self, data: dict) -> List[Tuple[Optional[str], Optional[bytes]]]:
        """解析图片生成 API 响应，返回 [(url, bytes), ...]"""
        results = []
        # 标准 OpenAI 格式: {"data": [{"url": "..."} or {"b64_json": "..."}]}
        if "data" in data and isinstance(data["data"], list):
            for item in data["data"]:
                if isinstance(item, dict):
                    if item.get("url"):
                        results.append((item["url"], None))
                    elif item.get("b64_json"):
                        try:
                            img_bytes = base64.b64decode(item["b64_json"])
                            results.append((None, img_bytes))
                        except Exception as e:
                            logger.warning(f"Base64 解码失败: {e}")

        # chat/completions 变体：choices[0].message.content 直接是 base64 字符串
        if not results and isinstance(data.get("choices"), list):
            for choice in data.get("choices") or []:
                msg = choice.get("message") if isinstance(choice, dict) else None
                if isinstance(msg, dict):
                    content = msg.get("content")
                    if isinstance(content, str) and content.strip():
                        content = content.strip()
                        extracted_url = self._extract_url_from_text(content)
                        if extracted_url:
                            results.append((extracted_url, None))
                            break
                        extracted_b64 = self._extract_base64_from_text(content)
                        if extracted_b64:
                            try:
                                img_bytes = base64.b64decode(extracted_b64)
                                results.append((None, img_bytes))
                                break
                            except Exception as e:
                                logger.warning(f"Base64 解码失败: {e}")

        # 其他格式: 尝试提取 URL 或 Base64
        if not results:
            url, b64, _ = self._parse_json_response(data)
            if url:
                results.append((url, None))
            elif b64:
                try:
                    img_bytes = base64.b64decode(b64)
                    results.append((None, img_bytes))
                except Exception as e:
                    logger.warning(f"Base64 解码失败: {e}")

        return results

    @staticmethod
    def _extract_api_error_message(raw_text: str) -> str:
        """从 API 错误响应中提取可读信息"""
        if not raw_text:
            return ""

        text = raw_text.strip()
        if not text:
            return ""

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return text[:500]

        if isinstance(data, dict):
            error_obj = data.get("error")
            if isinstance(error_obj, dict):
                message = str(error_obj.get("message", "")).strip()
                code = str(error_obj.get("code", "")).strip()
                param = str(error_obj.get("param", "")).strip()
                parts = []
                if message:
                    parts.append(message)
                if code and code not in message:
                    parts.append(f"code={code}")
                if param and param not in message:
                    parts.append(f"param={param}")
                if parts:
                    return " | ".join(parts)
            elif isinstance(error_obj, str) and error_obj.strip():
                return error_obj.strip()

            for key in ("message", "detail", "error_description"):
                value = data.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

        return text[:500]

    @staticmethod
    def _is_size_related_error(error_message: str) -> bool:
        """判断是否是尺寸参数相关错误"""
        if not error_message:
            return False
        err = error_message.lower()
        if "invalid_size" in err or "size must be" in err:
            return True
        return "size" in err and (
            "invalid" in err
            or "unsupported" in err
            or "unknown" in err
            or "must be" in err
        )

    @staticmethod
    def _is_resolution_related_error(error_message: str) -> bool:
        """判断是否是视频分辨率参数相关错误"""
        if not error_message:
            return False
        err = error_message.lower()
        if "invalid_resolution" in err:
            return True
        if "resolution_name" in err:
            return True
        return "resolution" in err and (
            "invalid" in err
            or "unsupported" in err
            or "must be" in err
        )

    @staticmethod
    def _is_response_format_related_error(error_message: str) -> bool:
        """判断是否是媒体格式参数相关错误"""
        if not error_message:
            return False
        err = error_message.lower()
        if "response_format" in err:
            return True
        return "format" in err and (
            "invalid" in err
            or "unsupported" in err
            or "must be" in err
        )

    @classmethod
    def _is_retryable_status(cls, status_code: int) -> bool:
        """判断状态码是否适合自动重试"""
        return status_code in cls.RETRYABLE_HTTP_STATUS_CODES

    @staticmethod
    def _retry_delay_seconds(attempt_index: int) -> float:
        """退避重试等待时长"""
        return min(1.5 * (2 ** attempt_index), 4.0)

    @staticmethod
    def _safe_int_conf(value: Any, default: int, minimum: int = 1) -> int:
        try:
            parsed = int(value)
        except Exception:
            parsed = default
        return max(minimum, parsed)

    def _get_media_file_limit(self, media_type: str, persistent: bool) -> int:
        if persistent:
            if media_type == "video":
                return self._safe_int_conf(self.conf.get("max_cached_videos", 10), 10)
            return self._safe_int_conf(self.conf.get("max_cached_images", 30), 30)
        return self._safe_int_conf(self.conf.get("max_temp_media_files", 20), 20)

    def _get_media_directory(self, media_type: str, persistent: bool) -> Path:
        if persistent:
            return self.video_dir if media_type == "video" else self.image_dir
        return self.temp_video_dir if media_type == "video" else self.temp_image_dir

    def _cleanup_directory_to_limit(self, directory: Path, limit: int) -> None:
        try:
            directory.mkdir(exist_ok=True, parents=True)
            candidates = sorted(
                [item for item in directory.iterdir() if item.is_file()],
                key=lambda item: item.stat().st_mtime,
                reverse=True,
            )
            for stale in candidates[limit:]:
                try:
                    stale.unlink()
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"清理目录缓存失败: dir={directory}, err={e}")

    async def _write_temp_media_file(
        self,
        media_bytes: bytes,
        media_type: str,
        suffix: str,
    ) -> str:
        folder = self._get_media_directory(media_type, persistent=False)
        folder.mkdir(exist_ok=True, parents=True)
        filename = f"grok_{media_type}_{int(time.time())}_{uuid.uuid4().hex[:8]}{suffix}"
        file_path = folder / filename
        async with aiofiles.open(file_path, "wb") as f:
            await f.write(media_bytes)
        self._cleanup_directory_to_limit(
            folder,
            self._get_media_file_limit(media_type, persistent=False),
        )
        return str(file_path)

    @staticmethod
    def _clean_param_token(token: str) -> str:
        cleaned = (token or "").strip().strip(",，。;；|")
        cleaned = cleaned.strip("()[]{}<>")
        if cleaned.endswith("的") and len(cleaned) > 1:
            cleaned = cleaned[:-1]
        return cleaned.strip()

    @staticmethod
    def _looks_like_size_token(token: str) -> bool:
        value = (token or "").strip().lower()
        if not value:
            return False
        return bool(
            re.fullmatch(r"\d{1,2}:\d{1,2}", value)
            or re.fullmatch(r"\d{3,4}x\d{3,4}", value)
        )

    def _resolve_video_timeout(
        self,
        video_length: int,
        stream: bool,
        resolution_name: Optional[str] = None,
    ) -> int:
        rounds = max(1, (max(video_length, self.DEFAULT_VIDEO_LENGTH_SECONDS) + 5) // 6)
        timeout = int(self.VIDEO_TIMEOUT * rounds)
        resolved_resolution = str(
            resolution_name or self._get_default_video_resolution_name()
        ).strip().lower()
        if resolved_resolution == "720p":
            timeout += 180 + max(0, rounds - 1) * 60
        if stream:
            timeout += max(0, rounds - 1) * 60
        return max(self.VIDEO_TIMEOUT, timeout)

    @staticmethod
    def _is_gateway_timeout_like_error(
        status_code: Optional[int],
        error_message: Optional[str],
    ) -> bool:
        if status_code == 504:
            return True
        err = str(error_message or "").lower()
        return any(
            marker in err
            for marker in (
                "504",
                "gateway timeout",
                "网关超时",
                "请求超时",
                "timeout",
                "openresty",
                "upstream_error",
                "stream idle timeout",
                "未能从响应中提取媒体内容",
                "未返回有效视频内容",
            )
        )

    def _get_public_auth_candidates(self) -> List[Optional[str]]:
        candidates: List[Optional[str]] = []
        seen = set()
        raw_values = (
            str(
                self.conf.get(
                    "grok_public_key",
                    self.conf.get("grok_function_key", ""),
                )
                or ""
            ).strip(),
            str(self.conf.get("grok_api_key", "") or "").strip(),
            "",
        )
        for value in raw_values:
            normalized = value or None
            marker = normalized or "__none__"
            if marker in seen:
                continue
            seen.add(marker)
            candidates.append(normalized)
        return candidates

    @staticmethod
    def _build_optional_bearer_headers(api_key: Optional[str]) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        return headers

    @staticmethod
    def _normalize_public_video_length(video_length: int) -> int:
        value = int(video_length or 6)
        if value <= 6:
            return 6
        if value <= 10:
            return 10
        return 15

    @classmethod
    def _parse_video_length_token(cls, token: str) -> Optional[int]:
        """解析视频时长参数，支持 6-30、6s、10秒、15sec 等格式"""
        if not token:
            return None
        cleaned = token.strip().lower()
        cleaned = re.sub(r"(seconds?|secs?|sec|秒)$", "", cleaned)
        if cleaned.endswith("s"):
            cleaned = cleaned[:-1]
        if not cleaned.isdigit():
            return None
        value = int(cleaned)
        if value in cls.SUPPORTED_VIDEO_LENGTH_SECONDS:
            return value
        return None

    @staticmethod
    def _segment_type_name(seg: Any) -> str:
        if not seg:
            return ""
        containers: List[Any] = [seg]
        if isinstance(seg, dict):
            data = seg.get("data")
            if isinstance(data, dict):
                containers.append(data)
        else:
            data = getattr(seg, "data", None)
            if data is not None:
                containers.append(data)

        for container in containers:
            if isinstance(container, dict):
                for key in ("type", "segment_type", "message_type"):
                    value = container.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip().lower()
            else:
                for key in ("type", "segment_type", "message_type"):
                    value = getattr(container, key, None)
                    if isinstance(value, str) and value.strip():
                        return value.strip().lower()
        return seg.__class__.__name__.lower()

    @staticmethod
    def _get_segment_attr(seg: Any, attr: str) -> Any:
        if seg is None:
            return None
        if isinstance(seg, dict):
            if attr in seg:
                return seg.get(attr)
            data = seg.get("data")
            if isinstance(data, dict) and attr in data:
                return data.get(attr)
            return None
        value = getattr(seg, attr, None)
        if value is not None:
            return value
        data = getattr(seg, "data", None)
        if data is not None:
            if isinstance(data, dict):
                return data.get(attr)
            return getattr(data, attr, None)
        return None

    def _get_segment_field(self, seg: Any, *keys: str) -> Any:
        for key in keys:
            value = self._get_segment_attr(seg, key)
            if value is not None and str(value).strip():
                return value
        return None

    def _is_segment_type(self, seg: Any, type_name: str) -> bool:
        """兼容不同平台实现的消息段类型判断"""
        cls = getattr(Comp, type_name, None)
        if cls is not None:
            try:
                if isinstance(seg, cls):
                    return True
            except Exception:
                pass
        seg_type = self._segment_type_name(seg)
        target = type_name.lower()
        aliases = {target}
        if target == "reply":
            aliases.update({"quote", "reference"})
            return any(alias in seg_type for alias in aliases)
        if target == "image":
            aliases.update({"img", "picture", "photo"})
        return seg_type in aliases or seg_type.endswith(target)

    @staticmethod
    def _dedupe_strings(values: List[str]) -> List[str]:
        result: List[str] = []
        seen = set()
        for value in values:
            if not isinstance(value, str):
                continue
            cleaned = value.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            result.append(cleaned)
        return result

    def _load_generated_image_url_index(self) -> Dict[str, Dict[str, Any]]:
        try:
            if not self.generated_image_index_path.is_file():
                return {}
            data = json.loads(self.generated_image_index_path.read_text(encoding="utf-8"))
            if not isinstance(data, dict):
                return {}
            result: Dict[str, Dict[str, Any]] = {}
            changed = False
            for key, value in data.items():
                if not isinstance(key, str) or not isinstance(value, dict):
                    continue
                url = value.get("url")
                ts = value.get("ts")
                if isinstance(url, str) and url.strip():
                    normalized_url = self._infer_generated_passthrough_url(url.strip()) or url.strip()
                    if normalized_url != url.strip():
                        changed = True
                    result[key] = {
                        "url": normalized_url,
                        "ts": int(ts) if isinstance(ts, (int, float)) else int(time.time()),
                    }
            if changed:
                self.generated_image_index_path.write_text(
                    json.dumps(result, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            return result
        except Exception as e:
            logger.warning(f"加载文生图 URL 索引失败: {e}")
            return {}

    def _save_generated_image_url_index(self) -> None:
        try:
            items = sorted(
                self._generated_image_url_index.items(),
                key=lambda item: int(item[1].get("ts", 0)),
                reverse=True,
            )[: self.GENERATED_IMAGE_URL_INDEX_LIMIT]
            payload = {key: value for key, value in items}
            self.generated_image_index_path.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
        except Exception as e:
            logger.warning(f"保存文生图 URL 索引失败: {e}")

    @staticmethod
    def _hash_bytes(payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()

    def _normalize_remote_url(self, source: Optional[str]) -> Optional[str]:
        if not isinstance(source, str):
            return None
        source = source.strip()
        if not source:
            return None
        if source.startswith(("http://", "https://", "data:")):
            return source
        if source.startswith("/"):
            return f"{self._get_base_url().rstrip('/')}{source}"
        return None

    def _infer_generated_passthrough_url(self, source: Optional[str]) -> Optional[str]:
        normalized = self._normalize_remote_url(source)
        if not normalized:
            return None
        if normalized.startswith("data:"):
            return normalized
        parsed = urlparse(normalized)
        path = parsed.path or ""
        if path.startswith("/v1/files/image/"):
            filename = Path(path).name
            suffix = Path(filename).suffix.lower().lstrip(".")
            if suffix == "jpeg":
                suffix = "jpg"
            if suffix in {"jpg", "png", "webp"}:
                image_id_matches = re.findall(
                    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
                    path,
                    re.IGNORECASE,
                )
                if image_id_matches:
                    image_id = image_id_matches[-1].lower()
                    return f"https://imagine-public.x.ai/imagine-public/images/{image_id}.{suffix}"
        return normalized

    @staticmethod
    def _extract_parent_post_id_from_source(source: Optional[str]) -> Optional[str]:
        text = str(source or "").strip()
        if not text:
            return None
        matches = re.findall(
            r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
            text,
            re.IGNORECASE,
        )
        if not matches:
            return None
        return matches[-1].lower()

    def _remember_generated_image_url(
        self,
        image_bytes: Optional[bytes],
        source: Optional[str],
        *,
        prefer_generated_url: bool = False,
    ) -> Optional[str]:
        if not image_bytes:
            return None
        passthrough_url = (
            self._infer_generated_passthrough_url(source)
            if prefer_generated_url
            else self._normalize_remote_url(source)
        )
        if not passthrough_url:
            return None
        digest = self._hash_bytes(image_bytes)
        self._generated_image_url_index[digest] = {
            "url": passthrough_url,
            "ts": int(time.time()),
        }
        self._save_generated_image_url_index()
        return passthrough_url

    def _lookup_generated_image_url(self, image_bytes: Optional[bytes]) -> Optional[str]:
        if not image_bytes:
            return None
        digest = self._hash_bytes(image_bytes)
        item = self._generated_image_url_index.get(digest)
        if not isinstance(item, dict):
            return None
        url = item.get("url")
        if isinstance(url, str) and url.strip():
            normalized_url = self._infer_generated_passthrough_url(url.strip()) or url.strip()
            if normalized_url != url.strip():
                item["url"] = normalized_url
                item["ts"] = int(time.time())
                self._save_generated_image_url_index()
            return normalized_url
        return None

    @classmethod
    def _extract_segment_sources(cls, seg: Any) -> List[str]:
        sources: List[str] = []
        candidate_keys = (
            "file",
            "url",
            "path",
            "src",
            "file_unique",
            "file_id",
            "id",
            "image",
            "image_url",
            "file_url",
            "download_url",
            "origin_url",
            "proxy_url",
        )

        def collect_from(value: Any):
            if isinstance(value, str):
                sources.append(value)
                return
            if isinstance(value, dict):
                for key in candidate_keys:
                    collect_from(value.get(key))
                return
            if isinstance(value, (list, tuple)):
                for item in value:
                    collect_from(item)

        containers: List[Any] = [seg]
        data = seg.get("data") if isinstance(seg, dict) else getattr(seg, "data", None)
        if isinstance(data, dict):
            containers.append(data)

        for container in containers:
            if isinstance(container, dict):
                for key in candidate_keys:
                    collect_from(container.get(key))
            else:
                for key in candidate_keys:
                    collect_from(getattr(container, key, None))

        return cls._dedupe_strings(sources)

    @staticmethod
    def _guess_filename_from_source(source: Optional[str], fallback: str) -> str:
        if not source:
            return fallback
        try:
            if source.startswith("http"):
                parsed = urlparse(source)
                candidate = unquote(Path(parsed.path).name)
            else:
                candidate = Path(source).name
            if candidate:
                return candidate
        except Exception:
            pass
        return fallback

    @staticmethod
    def _guess_mime_type_from_source(source: Optional[str], default: str) -> str:
        if source:
            guess, _ = mimetypes.guess_type(source)
            if guess:
                return guess
        return default

    @classmethod
    def _guess_audio_format_from_source(cls, source: Optional[str]) -> str:
        if not source:
            return "mp3"
        name = source.split("?", 1)[0].lower()
        if name.endswith(".wav"):
            return "wav"
        if name.endswith(".flac"):
            return "flac"
        if name.endswith(".ogg"):
            return "ogg"
        if name.endswith(".m4a"):
            return "m4a"
        if name.endswith(".aac"):
            return "aac"
        if name.endswith(".opus"):
            return "opus"
        if name.endswith(".mp3"):
            return "mp3"
        return "mp3"

    @staticmethod
    def _parse_size_string(size: str) -> Optional[Tuple[int, int]]:
        """解析 WxH 字符串"""
        if not size or "x" not in size.lower():
            return None
        try:
            width_str, height_str = size.lower().split("x", 1)
            width = int(width_str.strip())
            height = int(height_str.strip())
            if width <= 0 or height <= 0:
                return None
            return width, height
        except (ValueError, AttributeError):
            return None

    @staticmethod
    def _format_size(width: int, height: int) -> str:
        """格式化尺寸字符串"""
        return f"{width}x{height}"

    def _normalize_supported_size(self, size: str) -> Optional[str]:
        """归一化并校验是否为受支持尺寸

        支持两种输入格式：
        1. 像素格式：如 "1280x720"
        2. 比例格式：如 "16:9"

        Returns:
            标准化的像素格式字符串，如 "1280x720"；无效输入返回 None
        """
        if not size:
            return None
        size = str(size).strip()
        if not size:
            return None
        size = self.RATIO_ALIASES.get(size, size).replace("：", ":")

        # 先尝试比例格式（如 "16:9"）
        if ":" in size:
            parts = size.split(":")
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                # 有效的比例格式，查找对应的像素尺寸
                if size in self.ASPECT_RATIO_TO_SIZE:
                    return self.ASPECT_RATIO_TO_SIZE[size]
                return None

        # 尝试像素格式（如 "1280x720"）
        parsed = self._parse_size_string(size)
        if not parsed:
            return None
        normalized = self._format_size(parsed[0], parsed[1])
        if normalized in self.SUPPORTED_IMAGE_SIZES:
            return normalized
        return None

    def _get_default_image_size(self) -> str:
        configured_ratio = self._normalize_supported_size(
            str(self.conf.get("default_image_ratio", "")).strip()
        )
        return configured_ratio or self.DEFAULT_TEXT_IMAGE_SIZE

    def _get_default_video_size(self) -> str:
        configured_ratio = self._normalize_supported_size(
            str(self.conf.get("default_video_ratio", "")).strip()
        )
        return configured_ratio or self.DEFAULT_VIDEO_SIZE

    def _get_default_video_length_seconds(self) -> int:
        value = self._safe_int_conf(
            self.conf.get("default_video_length_seconds", self.DEFAULT_VIDEO_LENGTH_SECONDS),
            self.DEFAULT_VIDEO_LENGTH_SECONDS,
            minimum=min(self.SUPPORTED_VIDEO_LENGTH_SECONDS),
        )
        if value not in self.SUPPORTED_VIDEO_LENGTH_SECONDS:
            return self.DEFAULT_VIDEO_LENGTH_SECONDS
        return value

    def _get_default_video_resolution_name(self) -> str:
        value = str(
            self.conf.get("default_video_resolution", self.VIDEO_RESOLUTION_NAME)
        ).strip().lower()
        if value in {"480p", "720p"}:
            return value
        return self.VIDEO_RESOLUTION_NAME

    def _extract_stream_param_from_text(self, text: str) -> Tuple[str, Optional[bool]]:
        stream = None
        updated = text
        stream_patterns = (
            (r"(?<!\S)(?:非流式|关闭流式|nostream|nonstream|sync)(?!\S)", False),
            (r"(?<!\S)(?:流式|stream|sse)(?!\S)", True),
        )
        for pattern, value in stream_patterns:
            if re.search(pattern, updated, re.IGNORECASE):
                updated = re.sub(pattern, " ", updated, count=1, flags=re.IGNORECASE)
                stream = value
                break
        return re.sub(r"\s+", " ", updated).strip(), stream

    def _extract_size_param_from_text(self, text: str) -> Tuple[str, Optional[str], Optional[str]]:
        updated = text
        size = None
        invalid_size = None

        alias_pattern = r"(?:16[:：]9|9[:：]16|3[:：]2|2[:：]3|1[:：]1|横屏|横版|横构图|竖屏|竖版|竖构图|方图|方形|正方形)"
        match = re.search(rf"(?<!\w)({alias_pattern})(?:的)?(?!\w)", updated, re.IGNORECASE)
        if match:
            normalized = self._normalize_supported_size(match.group(1))
            if normalized:
                updated = updated[:match.start()] + " " + updated[match.end():]
                size = normalized
                return re.sub(r"\s+", " ", updated).strip(), size, None

        match = re.search(r"(?<!\w)(\d{3,4}x\d{3,4})(?:的)?(?!\w)", updated, re.IGNORECASE)
        if match:
            normalized = self._normalize_supported_size(match.group(1))
            updated = updated[:match.start()] + " " + updated[match.end():]
            if normalized:
                size = normalized
            else:
                invalid_size = match.group(1)
            return re.sub(r"\s+", " ", updated).strip(), size, invalid_size

        match = re.search(r"(?<!\w)(\d{1,2}[:：]\d{1,2})(?:的)?(?!\w)", updated, re.IGNORECASE)
        if match:
            normalized = self._normalize_supported_size(match.group(1))
            updated = updated[:match.start()] + " " + updated[match.end():]
            if normalized:
                size = normalized
            else:
                invalid_size = match.group(1)
            return re.sub(r"\s+", " ", updated).strip(), size, invalid_size

        return re.sub(r"\s+", " ", updated).strip(), None, None

    def _extract_video_length_from_text(self, text: str) -> Tuple[str, Optional[int]]:
        updated = text
        match = re.search(
            r"(?<!\d)((?:[6-9])|(?:[12]\d)|30)\s*(?:秒|seconds?|secs?|sec|s)(?:的)?",
            updated,
            re.IGNORECASE,
        )
        if match:
            value = self._parse_video_length_token(match.group(0))
            updated = updated[:match.start()] + " " + updated[match.end():]
            return re.sub(r"\s+", " ", updated).strip(), value

        match = re.match(r"^\s*((?:[6-9])|(?:[12]\d)|30)(?!\s*[:：])\b", updated)
        if match:
            value = self._parse_video_length_token(match.group(1))
            updated = updated[match.end():]
            return re.sub(r"\s+", " ", updated).strip(), value

        return re.sub(r"\s+", " ", updated).strip(), None

    def _get_image_resolution(self, image_bytes: bytes) -> Optional[Tuple[int, int]]:
        """读取图片分辨率"""
        if not Image:
            return None
        try:
            with Image.open(io.BytesIO(image_bytes)) as img:
                width, height = img.size
            if width <= 0 or height <= 0:
                return None
            return width, height
        except Exception as e:
            logger.warning(f"读取图片分辨率失败: {e}")
            return None

    def _get_closest_supported_size(self, width: int, height: int) -> Optional[str]:
        """按分辨率距离匹配最接近的合法尺寸"""
        if width <= 0 or height <= 0:
            return None

        candidates: List[Tuple[str, int, int]] = []
        for size_str in self.SUPPORTED_IMAGE_SIZES:
            parsed = self._parse_size_string(size_str)
            if parsed:
                candidates.append((size_str, parsed[0], parsed[1]))

        if not candidates:
            return None

        target_ratio = width / height
        target_area = width * height

        def distance(item: Tuple[str, int, int]) -> Tuple[float, float, float]:
            _, cand_w, cand_h = item
            dim_distance = (
                abs(cand_w - width) / max(width, 1)
                + abs(cand_h - height) / max(height, 1)
            )
            ratio_distance = abs((cand_w / cand_h) - target_ratio)
            area_distance = abs((cand_w * cand_h) - target_area) / max(target_area, 1)
            # 比例优先：先保证构图比例，再考虑像素接近度
            return ratio_distance, area_distance, dim_distance

        best = min(candidates, key=distance)
        return best[0]


    def _enforce_output_ratio_if_needed(self, image_bytes: bytes, target_size: Optional[str]) -> bytes:
        """
        将输出图像按 target_size 对应的宽高比进行兜底校正（仅补边，不裁切）。
        用于上游模型未严格遵循 size/ratio 时，保证最终发送给用户的比例稳定。
        """
        if not image_bytes or not target_size or Image is None:
            return image_bytes

        parsed_target = self._parse_size_string(target_size)
        if not parsed_target:
            return image_bytes

        target_w, target_h = parsed_target
        if target_w <= 0 or target_h <= 0:
            return image_bytes

        target_ratio = target_w / target_h

        try:
            with Image.open(io.BytesIO(image_bytes)) as img:
                src_w, src_h = img.size
                if src_w <= 0 or src_h <= 0:
                    return image_bytes

                src_ratio = src_w / src_h
                if abs(src_ratio - target_ratio) <= 0.002:
                    return image_bytes

                if src_ratio > target_ratio:
                    # 图像偏宽：补高
                    new_w = src_w
                    new_h = int(round(src_w / target_ratio))
                else:
                    # 图像偏高：补宽
                    new_h = src_h
                    new_w = int(round(src_h * target_ratio))

                if new_w <= 0 or new_h <= 0:
                    return image_bytes

                has_alpha = 'A' in img.getbands()
                if has_alpha:
                    working = img.convert('RGBA')
                    canvas = Image.new('RGBA', (new_w, new_h), (0, 0, 0, 0))
                else:
                    working = img.convert('RGB')
                    canvas = Image.new('RGB', (new_w, new_h), (0, 0, 0))

                offset_x = (new_w - src_w) // 2
                offset_y = (new_h - src_h) // 2
                canvas.paste(working, (offset_x, offset_y))

                output = io.BytesIO()
                mime = self._detect_mime_type(image_bytes)
                if mime == 'image/jpeg':
                    canvas = canvas.convert('RGB')
                    canvas.save(output, format='JPEG', quality=95, optimize=True)
                elif mime == 'image/webp':
                    canvas.save(output, format='WEBP', quality=95, method=6)
                else:
                    canvas.save(output, format='PNG', optimize=True)
                return output.getvalue()
        except Exception as e:
            logger.warning(f"输出比例校正失败，已跳过: {e}")
            return image_bytes

    @staticmethod
    def _render_prompt_template(template: str, mapping: Dict[str, Any]) -> str:
        """渲染提示词模板：仅替换已知占位符，未知占位符保持原样"""
        result = str(template or "")
        for k, v in mapping.items():
            result = result.replace("{" + str(k) + "}", str(v))
        return result

    @staticmethod
    def _to_bool(value: Any, default: bool = False) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            val = value.strip().lower()
            if val in {"1", "true", "yes", "on", "y", "t"}:
                return True
            if val in {"0", "false", "no", "off", "n", "f"}:
                return False
        return default

    @staticmethod
    def _to_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _parse_json_config_dict(self, key: str) -> Dict[str, Any]:
        value = self.conf.get(key, {})
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return {}
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError as exc:
                logger.warning(f"配置项 {key} JSON 解析失败: {exc}")
                return {}
            if isinstance(parsed, dict):
                return parsed
        return {}

    @staticmethod
    def _extract_urls_from_text(text: str) -> List[str]:
        urls = re.findall(r"https?://[^\s)\]}>\"']+", text or "")
        seen = set()
        result: List[str] = []
        for url in urls:
            cleaned = url.rstrip(".,;:!?'\"")
            if cleaned and cleaned not in seen:
                seen.add(cleaned)
                result.append(cleaned)
        return result

    @staticmethod
    def _coerce_search_json(text: str) -> Optional[Dict[str, Any]]:
        if not text:
            return None
        cleaned = text.strip()
        if cleaned.startswith("```"):
            cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
            cleaned = re.sub(r"\s*```$", "", cleaned).strip()
        if not (cleaned.startswith("{") and cleaned.endswith("}")):
            return None
        try:
            parsed = json.loads(cleaned)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    def _parse_search_message(self, message_content: Any) -> Tuple[str, List[Dict[str, str]], str]:
        if isinstance(message_content, str):
            message_text = message_content.strip()
        elif isinstance(message_content, list):
            parts: List[str] = []
            for item in message_content:
                if isinstance(item, dict):
                    text = item.get("text")
                    if isinstance(text, str):
                        parts.append(text)
            message_text = "".join(parts).strip()
        else:
            message_text = str(message_content or "").strip()

        parsed = self._coerce_search_json(message_text)
        sources: List[Dict[str, str]] = []
        raw = ""

        if parsed is None:
            content = message_text
            raw = message_text
        else:
            content = str(parsed.get("content") or "").strip()
            source_list = parsed.get("sources")
            if isinstance(source_list, list):
                for item in source_list:
                    if isinstance(item, dict) and item.get("url"):
                        sources.append(
                            {
                                "url": str(item.get("url")),
                                "title": str(item.get("title") or ""),
                                "snippet": str(item.get("snippet") or ""),
                            }
                        )
            if not content:
                content = message_text

        if not sources:
            for url in self._extract_urls_from_text(content):
                sources.append({"url": url, "title": "", "snippet": ""})

        return content, sources, raw

    def _search_show_sources(self) -> bool:
        return self._to_bool(self.conf.get("grok_search_show_sources", False), False)

    def _search_max_sources(self) -> int:
        value = self._to_int(self.conf.get("grok_search_max_sources", 5), 5)
        return 5 if value < 0 else value

    def _search_skill_enabled(self) -> bool:
        return self._to_bool(self.conf.get("grok_search_enable_skill", False), False)

    async def _perform_web_search(
        self,
        query: str,
        multimodal_inputs: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        started = time.time()
        query = (query or "").strip()
        multimodal_inputs = multimodal_inputs or {}
        image_bytes: Optional[bytes] = multimodal_inputs.get("image_bytes")
        audio_inputs: List[Dict[str, str]] = multimodal_inputs.get("audio_inputs", []) or []
        file_inputs: List[Dict[str, str]] = multimodal_inputs.get("file_inputs", []) or []

        if not query and not image_bytes and not audio_inputs and not file_inputs:
            return {
                "ok": False,
                "error": "请输入问题内容",
                "content": "",
                "sources": [],
                "raw": "",
                "elapsed_ms": int((time.time() - started) * 1000),
            }

        if query and len(query) > self.MAX_PROMPT_LENGTH:
            return {
                "ok": False,
                "error": f"输入内容过长，最大支持 {self.MAX_PROMPT_LENGTH} 字符",
                "content": "",
                "sources": [],
                "raw": "",
                "elapsed_ms": int((time.time() - started) * 1000),
            }

        api_key = str(self.conf.get("grok_api_key", "")).strip()
        if not api_key:
            return {
                "ok": False,
                "error": "未配置 API 密钥",
                "content": "",
                "sources": [],
                "raw": "",
                "elapsed_ms": int((time.time() - started) * 1000),
            }

        configured_model = (
            str(self.conf.get("grok_search_model", self.DEFAULT_SEARCH_MODEL)).strip()
            or self.DEFAULT_SEARCH_MODEL
        )
        model = await self._resolve_model(
            configured_model=configured_model,
            fallback_models=[self.DEFAULT_SEARCH_MODEL, "grok-4", "grok-3"],
            scene="对话/搜索",
        )
        timeout = self._to_float(
            self.conf.get("grok_search_timeout_seconds", self.DEFAULT_SEARCH_TIMEOUT),
            self.DEFAULT_SEARCH_TIMEOUT,
        )
        if timeout <= 0:
            timeout = self.DEFAULT_SEARCH_TIMEOUT

        enable_thinking = self._to_bool(
            self.conf.get("grok_search_enable_thinking", True),
            True,
        )
        thinking_budget = self._to_int(
            self.conf.get(
                "grok_search_thinking_budget",
                self.DEFAULT_SEARCH_THINKING_BUDGET,
            ),
            self.DEFAULT_SEARCH_THINKING_BUDGET,
        )
        if thinking_budget < 0:
            thinking_budget = self.DEFAULT_SEARCH_THINKING_BUDGET

        search_mode = str(self.conf.get("grok_search_mode", "auto")).strip().lower()
        if search_mode not in {"auto", "on", "off"}:
            search_mode = "auto"

        if search_mode == "off":
            system_prompt = (
                "You are a helpful assistant. "
                "IMPORTANT: Do NOT use Markdown formatting - respond in plain text only."
            )
            search_parameters = None
        elif search_mode == "on":
            system_prompt = (
                "You are a web research assistant. Use live web search/browsing when answering. "
                "Return ONLY a single JSON object with keys: "
                "content (string), sources (array of objects with url/title/snippet when possible). "
                "Keep content concise and evidence-backed. "
                "IMPORTANT: Do NOT use Markdown formatting in the content field - use plain text only."
            )
            search_parameters = {"mode": "on"}
        else:
            system_prompt = (
                "You are a helpful assistant with web search capabilities. "
                "If the user's question requires up-to-date information, current events, or facts you're unsure about, "
                "use web search to find accurate information. "
                "When you do search, return a JSON object with keys: "
                "content (string), sources (array of objects with url/title/snippet when possible). "
                "For general questions that don't need web search, respond normally. "
                "IMPORTANT: Do NOT use Markdown formatting - respond in plain text only."
            )
            search_parameters = {"mode": "auto"}

        has_multimodal = bool(image_bytes or audio_inputs or file_inputs)
        if has_multimodal:
            user_content: Any = [
                {"type": "text", "text": query or "请分析我发送的内容"},
            ]
        else:
            user_content = query

        if image_bytes and has_multimodal:
            mime_type = self._detect_mime_type(image_bytes)
            base64_image = base64.b64encode(image_bytes).decode("utf-8")
            user_content.append(
                {
                    "type": "image_url",
                    "image_url": {"url": f"data:{mime_type};base64,{base64_image}"},
                }
            )

        if has_multimodal:
            for audio in audio_inputs:
                audio_data = str(audio.get("data", "")).strip()
                audio_format = str(audio.get("format", "mp3")).strip() or "mp3"
                if not audio_data:
                    continue
                user_content.append(
                    {
                        "type": "input_audio",
                        "input_audio": {"data": audio_data, "format": audio_format},
                    }
                )

            for file_item in file_inputs:
                file_payload: Dict[str, str] = {}
                file_url = str(file_item.get("url", "")).strip()
                file_data = str(file_item.get("data", "")).strip()
                if file_url:
                    file_payload["file_url"] = file_url
                elif file_data:
                    file_payload["file_data"] = file_data
                else:
                    continue

                filename = str(file_item.get("filename", "")).strip()
                if filename:
                    file_payload["filename"] = filename
                user_content.append({"type": "file", "file": file_payload})

        payload: Dict[str, Any] = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_content},
            ],
            "temperature": 0.2,
            "stream": False,
        }
        if search_parameters:
            payload["search_parameters"] = search_parameters

        if enable_thinking:
            payload["reasoning_effort"] = "high"
            if thinking_budget > 0:
                payload["reasoning_budget_tokens"] = thinking_budget

        extra_body = self._parse_json_config_dict("grok_search_extra_body")
        for key, value in extra_body.items():
            if key not in {"model", "messages", "stream"}:
                payload[key] = value

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        extra_headers = self._parse_json_config_dict("grok_search_extra_headers")
        for key, value in extra_headers.items():
            if str(key).lower() not in {"authorization", "content-type"}:
                headers[str(key)] = str(value)

        api_url = f"{self._get_base_url()}/v1/chat/completions"
        raw_text = ""

        for attempt in range(self.MAX_REQUEST_RETRIES):
            try:
                session = await self._ensure_session()
                async with session.post(
                    api_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=timeout),
                ) as resp:
                    raw_text = await resp.text()
                    if resp.status != 200:
                        logger.warning(f"[对话/搜索] HTTP {resp.status}: {raw_text[:500]}")
                        if (
                            self._is_retryable_status(resp.status)
                            and attempt < self.MAX_REQUEST_RETRIES - 1
                        ):
                            await asyncio.sleep(self._retry_delay_seconds(attempt))
                            continue
                        return {
                            "ok": False,
                            "error": self._translate_error(f"状态码: {resp.status}"),
                            "content": "",
                            "sources": [],
                            "raw": raw_text[:2000] if raw_text else "",
                            "elapsed_ms": int((time.time() - started) * 1000),
                        }

                try:
                    data = json.loads(raw_text)
                except json.JSONDecodeError:
                    return {
                        "ok": False,
                        "error": "响应解析失败，API 返回了非 JSON 格式的数据",
                        "content": "",
                        "sources": [],
                        "raw": raw_text[:2000] if raw_text else "",
                        "elapsed_ms": int((time.time() - started) * 1000),
                    }

                if "error" in data and isinstance(data.get("error"), (dict, str)):
                    error_info = data["error"]
                    error_msg = (
                        error_info.get("message", str(error_info))
                        if isinstance(error_info, dict)
                        else str(error_info)
                    )
                    return {
                        "ok": False,
                        "error": self._translate_error(error_msg),
                        "content": "",
                        "sources": [],
                        "raw": raw_text[:2000] if raw_text else "",
                        "elapsed_ms": int((time.time() - started) * 1000),
                    }

                choices = data.get("choices")
                if not choices or not isinstance(choices, list):
                    return {
                        "ok": False,
                        "error": "响应缺少 choices 字段",
                        "content": "",
                        "sources": [],
                        "raw": raw_text[:2000] if raw_text else "",
                        "elapsed_ms": int((time.time() - started) * 1000),
                    }

                choice = choices[0] if isinstance(choices[0], dict) else {}
                message = choice.get("message") if isinstance(choice, dict) else {}
                content, sources, raw = self._parse_search_message((message or {}).get("content"))

                if not content:
                    return {
                        "ok": False,
                        "error": "API 返回了空响应",
                        "content": "",
                        "sources": [],
                        "raw": raw_text[:2000] if raw_text else "",
                        "elapsed_ms": int((time.time() - started) * 1000),
                    }

                return {
                    "ok": True,
                    "content": content,
                    "sources": sources,
                    "raw": raw,
                    "model": data.get("model") or model,
                    "usage": data.get("usage") or {},
                    "elapsed_ms": int((time.time() - started) * 1000),
                }
            except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
                if attempt < self.MAX_REQUEST_RETRIES - 1:
                    await asyncio.sleep(self._retry_delay_seconds(attempt))
                    continue
                return {
                    "ok": False,
                    "error": self._translate_error(str(exc) or "请求超时，请稍后重试"),
                    "content": "",
                    "sources": [],
                    "raw": "",
                    "elapsed_ms": int((time.time() - started) * 1000),
                }
            except Exception as exc:
                if attempt < self.MAX_REQUEST_RETRIES - 1:
                    await asyncio.sleep(self._retry_delay_seconds(attempt))
                    continue
                logger.error(f"[联网搜索] 请求异常: {exc}")
                return {
                    "ok": False,
                    "error": self._translate_error(str(exc)),
                    "content": "",
                    "sources": [],
                    "raw": "",
                    "elapsed_ms": int((time.time() - started) * 1000),
                }

        return {
            "ok": False,
            "error": "请求失败，请稍后重试",
            "content": "",
            "sources": [],
            "raw": "",
            "elapsed_ms": int((time.time() - started) * 1000),
        }

    def _format_search_result(self, result: Dict[str, Any]) -> str:
        if not result.get("ok"):
            error = result.get("error", "未知错误")
            raw = result.get("raw", "")
            if raw:
                try:
                    error_data = json.loads(raw)
                    if isinstance(error_data.get("error"), dict):
                        error = error_data["error"].get("message", error)
                    elif isinstance(error_data.get("error"), str):
                        error = error_data["error"]
                except (json.JSONDecodeError, KeyError):
                    pass
            return f"❌ 请求失败: {error}"

        content = self._strip_markdown(str(result.get("content", "")))
        sources = result.get("sources", [])
        if not isinstance(sources, list):
            sources = []

        lines = [content]
        if self._search_show_sources() and sources:
            max_sources = self._search_max_sources()
            selected = sources[:max_sources] if max_sources > 0 else sources
            lines.append("\n来源:")
            for i, src in enumerate(selected, 1):
                url = src.get("url", "")
                title = src.get("title", "")
                if title:
                    lines.append(f"  {i}. {title}\n     {url}")
                else:
                    lines.append(f"  {i}. {url}")

        return "\n".join(lines)

    def _format_search_result_for_llm(self, result: Dict[str, Any]) -> str:
        if not result.get("ok"):
            error = result.get("error", "未知错误")
            raw = result.get("raw", "")
            return f"搜索失败: {error}\n{raw}" if raw else f"搜索失败: {error}"

        content = str(result.get("content", ""))
        sources = result.get("sources", [])
        if not isinstance(sources, list):
            sources = []
        lines = [f"搜索结果:\n{content}"]

        if self._search_show_sources() and sources:
            max_sources = self._search_max_sources()
            selected = sources[:max_sources] if max_sources > 0 else sources
            lines.append("\n参考来源:")
            for i, src in enumerate(selected, 1):
                url = src.get("url", "")
                title = src.get("title", "")
                snippet = src.get("snippet", "")
                if title:
                    lines.append(f"  {i}. {title}")
                    lines.append(f"     {url}")
                else:
                    lines.append(f"  {i}. {url}")
                if snippet:
                    lines.append(f"     {snippet}")

        return "\n".join(lines)

    def _nsfw_enabled(self) -> bool:
        """Return whether NSFW mode is enabled in config (for Grok2API)."""
        if "nsfw_enabled" in self.conf:
            return self._to_bool(self.conf.get("nsfw_enabled", False), False)
        return self._to_bool(self.conf.get("nsfw", False), False)

    def _build_image_prompt(self, user_prompt: str, is_edit: bool = False) -> str:
        key = "edit_prompt_template" if is_edit else "image_prompt_template"
        template = self.conf.get(key, "{user_prompt}")
        return self._render_prompt_template(template, {"user_prompt": user_prompt}).strip()

    def _build_video_prompt(
        self,
        prompt: str,
        has_reference_image: bool,
        video_length: Optional[int] = None,
        aspect_ratio: Optional[str] = None,
    ) -> str:
        """构建视频提示词（轻量增强，支持模板中的时长/比例占位符）"""
        enhancement_enabled = self._to_bool(self.conf.get("enable_video_prompt_enhancement", True), True)

        default_enhancement_hint = (
            "画面要求：高细节、清晰边缘、低噪点、运动稳定、时序一致。"
            "输出风格自然，不要过度锐化。"
        )
        enhancement_hint = str(self.conf.get("video_enhancement_hint", default_enhancement_hint) or "") if enhancement_enabled else ""

        if has_reference_image:
            default_consistency = "保持参考图主体身份、构图和色调风格一致。"
            consistency_hint = str(self.conf.get("video_consistency_hint_with_image", default_consistency) or "") if enhancement_enabled else ""
        else:
            default_consistency = "主体动作连贯，镜头转场平滑。"
            consistency_hint = str(self.conf.get("video_consistency_hint_without_image", default_consistency) or "") if enhancement_enabled else ""

        default_template = "{user_prompt}\n\n{enhancement_hint}{consistency_hint}"
        template = str(self.conf.get("video_prompt_template", default_template) or default_template)
        length_hint = f"时长：{int(video_length)}秒。" if video_length else ""
        ratio_hint = f"比例：{aspect_ratio}。" if aspect_ratio else ""

        rendered = self._render_prompt_template(
            template,
            {
                "user_prompt": prompt,
                "enhancement_hint": enhancement_hint,
                "consistency_hint": consistency_hint,
                "length_hint": length_hint,
                "ratio_hint": ratio_hint,
            },
        )
        return rendered.strip()

    async def _fetch_available_models(self) -> Optional[set]:
        """探测当前可用模型列表，带短时缓存"""
        now = time.time()
        async with self._models_cache_lock:
            cached_models = set(self._models_cache.get("models", set()))
            expires_at = float(self._models_cache.get("expires_at", 0.0))
            if cached_models and now < expires_at:
                return cached_models

        base_url = self._get_base_url()
        api_url = f"{base_url}/v1/models"
        try:
            session = await self._ensure_session()
            api_key = str(self.conf.get("grok_api_key", "")).strip()
            async with session.get(
                api_url,
                headers={"Authorization": f"Bearer {api_key}"},
                timeout=aiohttp.ClientTimeout(total=self.MODEL_PROBE_TIMEOUT),
            ) as resp:
                if resp.status != 200:
                    return None
                raw_text = await resp.text()
            data = json.loads(raw_text)
            model_ids: set = set()
            for item in data.get("data", []) if isinstance(data, dict) else []:
                if isinstance(item, dict):
                    model_id = str(item.get("id", "")).strip()
                    if model_id:
                        model_ids.add(model_id)
            if not model_ids:
                return None
            async with self._models_cache_lock:
                self._models_cache["models"] = model_ids
                self._models_cache["expires_at"] = time.time() + self.MODEL_CACHE_TTL_SECONDS
            return set(model_ids)
        except Exception:
            return None

    async def _resolve_model(
        self,
        configured_model: str,
        fallback_models: List[str],
        scene: str,
    ) -> str:
        """根据 /v1/models 自动选择可用模型，不可用时按候选回退"""
        preferred_model = str(configured_model or "").strip()
        if not preferred_model and fallback_models:
            preferred_model = fallback_models[0]

        candidates: List[str] = []
        for model_name in [preferred_model, *fallback_models]:
            model_name = str(model_name or "").strip()
            if model_name and model_name not in candidates:
                candidates.append(model_name)

        if not candidates:
            return preferred_model

        available_models = await self._fetch_available_models()
        if not available_models:
            return candidates[0]

        for candidate in candidates:
            if candidate in available_models:
                if candidate != candidates[0]:
                    logger.warning(
                        f"[{scene}] 配置模型不可用，自动回退为可用模型: {candidate}"
                    )
                return candidate

        logger.warning(f"[{scene}] 未命中可用候选模型，继续使用: {candidates[0]}")
        return candidates[0]

    # ==================== API 调用 ====================

    @staticmethod
    def _detect_mime_type(data: bytes) -> str:
        """检测图片 MIME 类型"""
        if data.startswith(b'\x89PNG\r\n\x1a\n'):
            return "image/png"
        if data.startswith(b'\xff\xd8\xff'):
            return "image/jpeg"
        if data.startswith((b'GIF87a', b'GIF89a')):
            return "image/gif"
        if data.startswith(b'RIFF') and len(data) > 12 and data[8:12] == b'WEBP':
            return "image/webp"
        if data.startswith(b'BM'):
            return "image/bmp"
        return "image/png"

    def _get_headers(self) -> dict:
        api_key = str(self.conf.get("grok_api_key", "")).strip()
        return {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

    def _get_base_url(self) -> str:
        """获取 API 基础 URL，自动处理常见的 URL 格式问题

        用户只需填写基础 URL（如 https://api.x.ai），
        会自动移除多余的路径后缀，返回纯净的基础 URL
        """
        url = str(self.conf.get("grok_api_url", "https://api.x.ai")).rstrip("/")
        # 移除常见的端点后缀，只保留基础 URL
        suffixes = [
            "/v1/chat/completions", "/v1/images/generations", "/v1/images/edits",
            "/v1/video/generations", "/chat/completions", "/images/generations",
            "/images/edits", "/video/generations", "/v1"
        ]
        for suffix in suffixes:
            if url.endswith(suffix):
                url = url[:-len(suffix)]
        return url.rstrip("/")

    async def _generate_image(
        self,
        prompt: str,
        image_bytes: Optional[bytes] = None,
        image_url: Optional[str] = None,
        mask_bytes: Optional[bytes] = None,
        n: int = 1,
        target_size: Optional[str] = None,
        stream_preference: Optional[bool] = None,
    ) -> Tuple[List[Tuple[Optional[str], Optional[bytes]]], Optional[str]]:
        """调用 Grok 生图 API，返回 [(url_or_path, bytes), ...] 或错误

        文生图: POST /v1/images/generations (JSON)
        图生图: POST /v1/chat/completions (JSON)
        """
        if image_bytes:
            return await self._edit_image_via_chat(
                prompt,
                image_bytes,
                image_url=image_url,
                n=n,
                target_size=target_size,
                mask_bytes=mask_bytes,
                stream_preference=stream_preference,
            )

        base_url = self._get_base_url()
        api_url = f"{base_url}/v1/images/generations"
        configured_model = self.conf.get("grok_image_model", "grok-imagine-1.0")
        model = await self._resolve_model(
            configured_model=configured_model,
            fallback_models=["grok-imagine-1.0"],
            scene="文生图",
        )

        nsfw_enabled = self._nsfw_enabled()
        last_error: Optional[str] = None

        prefer_stream = stream_preference
        if prefer_stream is None:
            prefer_stream = bool(self.conf.get("stream_enabled", True))
        stream_modes = [True, False] if prefer_stream and n in (1, 2) else [False]

        for stream_mode in stream_modes:
            for response_format in self.IMAGE_RESPONSE_FORMAT_CANDIDATES:
                format_changed = False
                payload = {
                    "model": model,
                    "prompt": self._build_image_prompt(prompt, is_edit=False),
                    "n": max(1, min(n, self.MAX_IMAGE_COUNT)),
                    "stream": stream_mode,
                }
                if target_size:
                    payload["size"] = target_size
                if response_format:
                    payload["response_format"] = response_format
                if nsfw_enabled:
                    payload["nsfw"] = True

                logger.info(f"[文生图] 完整请求参数: {payload}")
                for attempt in range(self.MAX_REQUEST_RETRIES):
                    try:
                        session = await self._ensure_session()
                        async with session.post(
                            api_url,
                            headers=self._get_headers(),
                            json=payload,
                            timeout=aiohttp.ClientTimeout(total=self.IMAGE_TIMEOUT),
                        ) as resp:
                            if resp.status != 200:
                                text = await resp.text()
                                logger.error(
                                    f"[文生图] API 请求失败 (状态码: {resp.status}): {text[:200]}"
                                )
                                detail = self._extract_api_error_message(text)
                                translated_error = self._translate_error(
                                    detail or f"状态码: {resp.status}"
                                )
                                last_error = translated_error

                                if (
                                    response_format
                                    and self._is_response_format_related_error(detail)
                                ):
                                    logger.warning(
                                        f"[文生图] 返回格式不兼容，自动切换模式重试: {detail[:120]}"
                                    )
                                    format_changed = True
                                    break

                                if (
                                    self._is_retryable_status(resp.status)
                                    and attempt < self.MAX_REQUEST_RETRIES - 1
                                ):
                                    await asyncio.sleep(self._retry_delay_seconds(attempt))
                                    continue
                                if detail and "no results" in detail.lower():
                                    logger.warning(
                                        f"[文生图] 上游未返回结果，切换返回格式重试: {detail[:120]}"
                                    )
                                    format_changed = True
                                    break
                                return [], translated_error

                            if payload.get("stream"):
                                media_bytes, media_url, error = await self._parse_media_response(resp, "image")
                                if error:
                                    if attempt < self.MAX_REQUEST_RETRIES - 1:
                                        await asyncio.sleep(self._retry_delay_seconds(attempt))
                                        continue
                                    last_error = error
                                    break
                                if media_bytes or media_url:
                                    return [(media_url, media_bytes)], None
                                return [], "未能从响应中提取图片"

                            raw_content = await resp.read()
                            logger.info(f"[图生图] 响应前500字节: {raw_content[:500]}")
                            try:
                                data = json.loads(raw_content.decode("utf-8"))
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                logger.error(f"JSON解析失败，响应前200字节: {raw_content[:200]}")
                                return [], "API响应格式异常"

                            results = self._parse_image_api_response(data)
                            if results:
                                return results, None
                            return [], "未能从响应中提取图片"

                    except (asyncio.TimeoutError, aiohttp.ClientError):
                        if attempt < self.MAX_REQUEST_RETRIES - 1:
                            await asyncio.sleep(self._retry_delay_seconds(attempt))
                            continue
                        last_error = "请求超时，请重试"
                    except Exception as e:
                        if attempt < self.MAX_REQUEST_RETRIES - 1:
                            await asyncio.sleep(self._retry_delay_seconds(attempt))
                            continue
                        logger.error(f"[文生图] 请求异常: {e}")
                        last_error = self._translate_error(str(e))

                if format_changed:
                    continue

        return [], last_error or "文生图请求失败"

    def _build_edit_image_form(
        self,
        model: str,
        prompt: str,
        n: int,
        image_bytes: bytes,
        size: Optional[str] = None,
        response_format: Optional[str] = "url",
        nsfw: bool = False,
        mask_bytes: Optional[bytes] = None,
    ) -> aiohttp.FormData:
        """构建图生图请求体"""
        form = aiohttp.FormData()
        form.add_field("model", model)
        form.add_field("prompt", prompt)
        form.add_field("n", str(max(1, min(n, self.MAX_IMAGE_COUNT))))
        if response_format:
            form.add_field("response_format", response_format)
        if nsfw:
            form.add_field("nsfw", "true")

        mime_type = self._detect_mime_type(image_bytes)
        ext = mime_type.split("/")[-1]
        if ext == "jpeg":
            ext = "jpg"
        form.add_field(
            "image",
            image_bytes,
            filename=f"image.{ext}",
            content_type=mime_type,
        )
        if mask_bytes:
            mask_mime_type = self._detect_mime_type(mask_bytes)
            mask_ext = mask_mime_type.split("/")[-1]
            if mask_ext == "jpeg":
                mask_ext = "jpg"
            form.add_field(
                "mask",
                mask_bytes,
                filename=f"mask.{mask_ext}",
                content_type=mask_mime_type,
            )
        return form

    async def _edit_image_via_chat(
        self,
        prompt: str,
        image_bytes: bytes,
        image_url: Optional[str] = None,
        n: int = 1,
        target_size: Optional[str] = None,
        mask_bytes: Optional[bytes] = None,
        stream_preference: Optional[bool] = None,
    ) -> Tuple[List[Tuple[Optional[str], Optional[bytes]]], Optional[str]]:
        """调用 Grok 图片编辑 API (图生图) via /v1/chat/completions"""
        base_url = self._get_base_url()
        api_url = f"{base_url}/v1/chat/completions"
        configured_model = self.conf.get("grok_edit_model", "grok-imagine-1.0-edit")
        model = await self._resolve_model(
            configured_model=configured_model,
            fallback_models=["grok-imagine-1.0-edit", "grok-imagine-1.0"],
            scene="图生图",
        )

        nsfw_enabled = self._nsfw_enabled()
        last_error: Optional[str] = None

        content_blocks: List[Dict[str, Any]] = [
            {"type": "text", "text": self._build_image_prompt(prompt, is_edit=True)}
        ]
        passthrough_image_url = self._infer_generated_passthrough_url(image_url)
        if passthrough_image_url:
            logger.info(f"[图生图] 输入图像使用 URL 透传: {passthrough_image_url[:200]}")
            content_blocks.append(
                {"type": "image_url", "image_url": {"url": passthrough_image_url}}
            )
        else:
            mime_type = self._detect_mime_type(image_bytes)
            base64_image = base64.b64encode(image_bytes).decode("utf-8")
            logger.info(
                f"[图生图] 输入图像使用 data URI: mime={mime_type}, bytes={len(image_bytes)}, b64_len={len(base64_image)}"
            )
            content_blocks.append(
                {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}}
            )

        # mask 目前仅在 edits 接口支持，chat/completions 文档未说明，先忽略
        if mask_bytes:
            logger.warning("[图生图] chat/completions 暂不支持 mask，已忽略")

        messages = [{"role": "user", "content": content_blocks}]

        image_config: Dict[str, Any] = {
            "n": max(1, min(n, self.MAX_IMAGE_COUNT)),
        }
        if target_size:
            image_config["size"] = target_size
        if nsfw_enabled:
            image_config["nsfw"] = True

        prefer_stream = stream_preference
        if prefer_stream is None:
            prefer_stream = bool(self.conf.get("stream_enabled", True))
        stream_modes = [True, False] if prefer_stream and n == 1 else [False]

        payload_candidates: List[Dict[str, Any]] = []
        for stream_mode in stream_modes:
            for response_format in self.IMAGE_RESPONSE_FORMAT_CANDIDATES:
                cfg = dict(image_config)
                if response_format:
                    cfg["response_format"] = response_format
                payload_candidates.append({
                    "model": model,
                    "messages": messages,
                    "stream": stream_mode,
                    "image_config": cfg,
                })

        for payload_index, payload in enumerate(payload_candidates):
            logger.info(
                f"[图生图] 尝试请求 {payload_index + 1}/{len(payload_candidates)}: "
                f"stream={payload.get('stream')}, format={payload['image_config'].get('response_format')}"
            )
            logger.info(f"[图生图] 完整请求参数: {payload}")
            for attempt in range(self.MAX_REQUEST_RETRIES):
                try:
                    session = await self._ensure_session()
                    async with session.post(
                        api_url,
                        headers=self._get_headers(),
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=self.IMAGE_TIMEOUT),
                    ) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            logger.error(f"[图生图] API 请求失败 (状态码: {resp.status}): {text[:200]}")
                            detail = self._extract_api_error_message(text)
                            translated_error = self._translate_error(detail or f"状态码: {resp.status}")
                            last_error = translated_error

                            if (
                                self._is_retryable_status(resp.status)
                                and attempt < self.MAX_REQUEST_RETRIES - 1
                            ):
                                await asyncio.sleep(self._retry_delay_seconds(attempt))
                                continue
                            break

                        if payload.get("stream"):
                            media_bytes, media_url, error = await self._parse_media_response(resp, "image")
                            if error:
                                if attempt < self.MAX_REQUEST_RETRIES - 1:
                                    await asyncio.sleep(self._retry_delay_seconds(attempt))
                                    continue
                                return [], error
                            if media_bytes or media_url:
                                return [(media_url, media_bytes)], None
                            return [], "未能从响应中提取图片"

                        raw_content = await resp.read()
                        logger.info(f"[图生图] 响应前500字节: {raw_content[:500]}")
                        try:
                            data = json.loads(raw_content.decode("utf-8"))
                        except (json.JSONDecodeError, UnicodeDecodeError):
                            logger.error(f"JSON解析失败，响应前200字节: {raw_content[:200]}")
                            return [], "API响应格式异常"

                        results = self._parse_image_api_response(data)
                        if results:
                            return results, None
                        return [], "未能从响应中提取图片"

                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt < self.MAX_REQUEST_RETRIES - 1:
                        await asyncio.sleep(self._retry_delay_seconds(attempt))
                        continue
                    last_error = "请求超时，请重试"
                except Exception as e:
                    if attempt < self.MAX_REQUEST_RETRIES - 1:
                        await asyncio.sleep(self._retry_delay_seconds(attempt))
                        continue
                    logger.error(f"[图生图] 请求异常: {e}")
                    last_error = self._translate_error(str(e))

        return [], last_error or "图生图请求失败"

    async def _start_public_video_task(
        self,
        api_url: str,
        payload: Dict[str, Any],
    ) -> Tuple[Optional[str], Optional[str]]:
        last_error: Optional[str] = None

        for auth_key in self._get_public_auth_candidates():
            headers = self._build_optional_bearer_headers(auth_key)
            headers["Content-Type"] = "application/json; charset=utf-8"
            auth_label = "public_key" if auth_key else "anonymous"
            try:
                session = await self._ensure_session()
                async with session.post(
                    api_url,
                    headers=headers,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30),
                ) as resp:
                    raw_text = await resp.text()
                    if resp.status != 200:
                        detail = self._extract_api_error_message(raw_text)
                        last_error = self._translate_error(detail or f"状态码: {resp.status}")
                        logger.warning(
                            f"[生视频] public/start 失败: status={resp.status}, auth={auth_label}, detail={detail[:200]}"
                        )
                        if resp.status in {401, 403}:
                            continue
                        return None, last_error

                try:
                    data = json.loads(raw_text)
                except json.JSONDecodeError:
                    last_error = "public/start 响应格式异常"
                    logger.warning(f"[生视频] public/start 非 JSON 响应: {raw_text[:200]}")
                    continue

                task_id = str(data.get("task_id", "")).strip() if isinstance(data, dict) else ""
                if task_id:
                    logger.info(f"[生视频] public/start 成功: auth={auth_label}, task_id={task_id}")
                    return task_id, None

                last_error = "public/start 未返回 task_id"
                logger.warning(f"[生视频] public/start 缺少 task_id: {raw_text[:200]}")
            except (asyncio.TimeoutError, aiohttp.ClientError):
                last_error = "public/start 请求超时，请重试"
            except Exception as e:
                last_error = self._translate_error(str(e))
                logger.warning(f"[生视频] public/start 请求异常: {e}")

        return None, last_error or "public/start 调用失败"

    async def _generate_video_via_public_api(
        self,
        prompt: str,
        image_bytes: Optional[bytes] = None,
        image_url: Optional[str] = None,
        target_size: Optional[str] = None,
        video_length: int = 6,
    ) -> Tuple[Optional[str], Optional[str]]:
        base_url = self._get_base_url()
        start_url = f"{base_url}/v1/public/video/start"
        sse_url = f"{base_url}/v1/public/video/sse"
        resolved_size = (
            self._normalize_supported_size(target_size or self.DEFAULT_VIDEO_SIZE)
            or self.DEFAULT_VIDEO_SIZE
        )
        aspect_ratio = self.SIZE_TO_ASPECT_RATIO.get(
            resolved_size,
            self.SIZE_TO_ASPECT_RATIO[self.DEFAULT_VIDEO_SIZE],
        )
        resolved_length = int(video_length or self.DEFAULT_VIDEO_LENGTH_SECONDS)
        public_length = self._normalize_public_video_length(resolved_length)
        enhanced_prompt = self._build_video_prompt(
            prompt,
            has_reference_image=bool(image_bytes or image_url),
            video_length=public_length,
            aspect_ratio=aspect_ratio,
        )

        normalized_image_url = self._infer_generated_passthrough_url(image_url)
        parent_post_id = self._extract_parent_post_id_from_source(normalized_image_url)
        payload: Dict[str, Any] = {
            "prompt": enhanced_prompt,
            "reasoning_effort": "low",
            "aspect_ratio": aspect_ratio,
            "video_length": public_length,
            "resolution_name": self._get_default_video_resolution_name(),
            "preset": "custom",
        }

        if normalized_image_url and parent_post_id:
            payload["parent_post_id"] = parent_post_id
            payload["source_image_url"] = normalized_image_url
        elif normalized_image_url:
            payload["image_url"] = normalized_image_url
            payload["source_image_url"] = normalized_image_url
        elif image_bytes:
            mime_type = self._detect_mime_type(image_bytes)
            base64_image = base64.b64encode(image_bytes).decode("utf-8")
            payload["image_url"] = f"data:{mime_type};base64,{base64_image}"
            payload["source_image_url"] = payload["image_url"]

        if public_length != resolved_length:
            logger.info(
                f"[生视频] public 视频回退时长已对齐新后端限制: requested={resolved_length}s, fallback={public_length}s"
            )
        logger.info(
            "[生视频] 尝试回退至 public 视频接口: "
            f"aspect_ratio={aspect_ratio}, video_length={public_length}, "
            f"has_reference={bool(image_bytes or image_url)}, parent_post_id={bool(parent_post_id)}"
        )

        task_id, start_error = await self._start_public_video_task(start_url, payload)
        if not task_id:
            return None, start_error

        try:
            session = await self._ensure_session()
            async with session.get(
                sse_url,
                params={"task_id": task_id},
                headers={"Accept": "text/event-stream"},
                timeout=aiohttp.ClientTimeout(
                    total=self._resolve_video_timeout(
                        public_length,
                        True,
                        payload["resolution_name"],
                    )
                ),
            ) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    detail = self._extract_api_error_message(text)
                    logger.warning(
                        f"[生视频] public/sse 失败: status={resp.status}, detail={detail[:200]}"
                    )
                    return None, self._translate_error(detail or f"状态码: {resp.status}")

                media_bytes, media_url, error = await self._parse_media_response(resp, "video")
                if error:
                    return None, error
                if media_bytes:
                    return await self._write_temp_media_file(
                        media_bytes,
                        media_type="video",
                        suffix=".mp4",
                    ), None
                if media_url:
                    return media_url, None
                return None, "public 视频接口未返回有效视频内容"
        except (asyncio.TimeoutError, aiohttp.ClientError):
            return None, "public 视频接口超时，请重试"
        except Exception as e:
            logger.warning(f"[生视频] public 视频接口异常: {e}")
            return None, self._translate_error(str(e))

    async def _generate_video(
        self,
        prompt: str,
        image_bytes: Optional[bytes] = None,
        image_url: Optional[str] = None,
        target_size: Optional[str] = None,
        video_length: int = 6,
        stream_preference: Optional[bool] = None,
    ) -> Tuple[Optional[str], Optional[str]]:
        """调用 Grok 生视频 API

        使用 /v1/chat/completions 接口，模型为 grok-imagine-1.0-video
        """
        base_url = self._get_base_url()
        api_url = f"{base_url}/v1/chat/completions"
        configured_model = self.conf.get("grok_video_model", "grok-imagine-1.0-video")
        model = await self._resolve_model(
            configured_model=configured_model,
            fallback_models=["grok-imagine-1.0-video"],
            scene="生视频",
        )
        scene_tag = "图生视频" if (image_bytes or image_url) else "文生视频"
        resolved_size = self._normalize_supported_size(target_size or self.DEFAULT_VIDEO_SIZE) or self.DEFAULT_VIDEO_SIZE
        aspect_ratio = self.SIZE_TO_ASPECT_RATIO.get(
            resolved_size,
            self.SIZE_TO_ASPECT_RATIO[self.DEFAULT_VIDEO_SIZE],
        )
        resolution_name = self._get_default_video_resolution_name()
        video_config = {
            "aspect_ratio": aspect_ratio,
            "video_length": int(video_length or self.DEFAULT_VIDEO_LENGTH_SECONDS),
            "resolution_name": resolution_name,
            "preset": "custom",
        }
        enhanced_prompt = self._build_video_prompt(
            prompt,
            has_reference_image=bool(image_bytes or image_url),
            video_length=video_config["video_length"],
            aspect_ratio=aspect_ratio,
        )
        content_blocks: List[Dict[str, Any]] = [{"type": "text", "text": enhanced_prompt}]
        passthrough_image_url = self._infer_generated_passthrough_url(image_url)
        if passthrough_image_url:
            logger.info(f"[{scene_tag}] 输入图像使用 URL 透传: {passthrough_image_url[:200]}")
            content_blocks.append(
                {"type": "image_url", "image_url": {"url": passthrough_image_url}}
            )
        elif image_bytes:
            try:
                mime_type = self._detect_mime_type(image_bytes)
                base64_image = base64.b64encode(image_bytes).decode("utf-8")
                logger.info(
                    f"[{scene_tag}] 输入图像使用 data URI: mime={mime_type}, bytes={len(image_bytes)}, b64_len={len(base64_image)}"
                )
                content_blocks.append(
                    {"type": "image_url", "image_url": {"url": f"data:{mime_type};base64,{base64_image}"}}
                )
            except Exception as e:
                logger.warning(f"[{scene_tag}] 构造 data URI 失败: {e}")

        messages = [{"role": "user", "content": content_blocks}]

        prefer_stream = stream_preference
        if prefer_stream is None:
            prefer_stream = bool(self.conf.get("stream_enabled", True))

        payload_candidates: List[Dict[str, Any]] = []
        stream_modes = [True, False] if prefer_stream else [False]
        for stream_mode in stream_modes:
            payload_candidates.append(
                {
                    "model": model,
                    "messages": messages,
                    "stream": stream_mode,
                    "video_config": dict(video_config),
                }
            )

        last_error: Optional[str] = None
        last_status_code: Optional[int] = None
        for payload_index, payload in enumerate(payload_candidates):
            logger.info(f"[生视频] 尝试请求 {payload_index + 1}/{len(payload_candidates)}: stream={payload.get('stream')}")
            logger.info(f"[生视频] 完整请求参数: {payload}")

            for attempt in range(self.MAX_REQUEST_RETRIES):
                try:
                    session = await self._ensure_session()
                    async with session.post(
                        api_url,
                        headers=self._get_headers(),
                        json=payload,
                        timeout=aiohttp.ClientTimeout(
                            total=self._resolve_video_timeout(
                                video_config["video_length"],
                                bool(payload.get("stream")),
                                video_config["resolution_name"],
                            )
                        )
                    ) as resp:
                        if resp.status != 200:
                            text = await resp.text()
                            logger.error(f"[{scene_tag}] API 请求失败 (状态码: {resp.status}): {text[:200]}")
                            detail = self._extract_api_error_message(text)
                            translated_error = self._translate_error(detail or f"状态码: {resp.status}")
                            last_error = translated_error
                            last_status_code = resp.status

                            if (
                                self._is_retryable_status(resp.status)
                                and attempt < self.MAX_REQUEST_RETRIES - 1
                            ):
                                await asyncio.sleep(self._retry_delay_seconds(attempt))
                                continue

                            break

                        media_bytes, media_url, error = await self._parse_media_response(resp, "video")
                        if error:
                            if attempt < self.MAX_REQUEST_RETRIES - 1:
                                await asyncio.sleep(self._retry_delay_seconds(attempt))
                                continue
                            last_error = error
                            break
                        if media_bytes:
                            return await self._write_temp_media_file(
                                media_bytes,
                                media_type="video",
                                suffix=".mp4",
                            ), None
                        if media_url:
                            return media_url, None
                        last_error = "API 响应中未包含有效视频内容"
                        break

                except (asyncio.TimeoutError, aiohttp.ClientError):
                    if attempt == self.MAX_REQUEST_RETRIES - 1:
                        last_error = "请求超时，请重试"
                    else:
                        await asyncio.sleep(self._retry_delay_seconds(attempt))
                except Exception as e:
                    if attempt == self.MAX_REQUEST_RETRIES - 1:
                        logger.error(f"[{scene_tag}] 请求异常: {e}")
                        last_error = self._translate_error(str(e))
                    else:
                        await asyncio.sleep(self._retry_delay_seconds(attempt))

        if self._is_gateway_timeout_like_error(last_status_code, last_error):
            fallback_path_or_url, fallback_error = await self._generate_video_via_public_api(
                prompt=prompt,
                image_bytes=image_bytes,
                image_url=image_url,
                target_size=target_size,
                video_length=video_config["video_length"],
            )
            if fallback_path_or_url:
                return fallback_path_or_url, None
            if fallback_error:
                logger.warning(f"[生视频] public 视频接口回退失败: {fallback_error}")
                if last_error:
                    last_error = f"{last_error}；public 回退失败：{fallback_error}"
                else:
                    last_error = fallback_error

        return None, last_error or "所有重试均失败"
    # ==================== 响应解析 ====================

    async def _parse_media_response(self, resp, media_type: str = "image") -> Tuple[Optional[bytes], Optional[str], Optional[str]]:
        """
        统一解析媒体响应，支持流式/非流式、URL/Base64
        返回: (media_bytes, media_url, error_msg)
        """
        accumulated_text = []
        is_streaming = False
        raw_content = b""
        extracted_url = None
        extracted_base64 = None
        line_count = 0

        while True:
            line = await resp.content.readline()
            if line == b"":
                break
            line_count += 1
            if line_count > self.MAX_STREAM_LINES:
                return None, None, "响应行数超限"
            if len(raw_content) > self.MAX_RESPONSE_BYTES:
                return None, None, "响应数据过大"
            raw_content += line
            if not line or not line.strip():
                continue

            try:
                line_str = line.decode('utf-8').strip()
            except UnicodeDecodeError:
                continue

            # SSE 流式解析
            payload_str = None
            if line_str.startswith('data: '):
                payload_str = line_str[6:]
            elif line_str.startswith('data:'):
                payload_str = line_str[5:]

            if payload_str is not None:
                is_streaming = True
                payload_str = payload_str.strip()
                if payload_str in ('[DONE]', 'done', ''):
                    continue

                try:
                    chunk = json.loads(payload_str)
                    # 提取文本内容
                    content = self._extract_text_content(chunk)
                    if content:
                        accumulated_text.append(content)
                    # 提取媒体数据
                    url, b64 = self._extract_media_from_chunk(chunk)
                    if url:
                        extracted_url = url
                    if b64:
                        extracted_base64 = b64
                except json.JSONDecodeError:
                    if payload_str.startswith(("http://", "https://")):
                        extracted_url = payload_str.split()[0]
                    elif self._is_base64(payload_str):
                        extracted_base64 = payload_str

        # 非流式响应处理
        if not is_streaming and raw_content:
            try:
                data = json.loads(raw_content.decode('utf-8'))
                # 处理各种 API 响应格式
                url, b64, text = self._parse_json_response(data)
                if url:
                    extracted_url = url
                if b64:
                    extracted_base64 = b64
                if text:
                    accumulated_text.append(text)
            except (json.JSONDecodeError, UnicodeDecodeError):
                pass

        # 从累积文本中提取媒体
        full_text = "".join(accumulated_text)
        if not extracted_url and not extracted_base64 and full_text:
            extracted_url = self._extract_url_from_text(full_text)
            if not extracted_url:
                extracted_base64 = self._extract_base64_from_text(full_text)

        # 返回结果
        if extracted_base64:
            try:
                media_bytes = base64.b64decode(extracted_base64)
                return media_bytes, None, None
            except Exception as e:
                return None, None, f"Base64 解码失败: {e}"

        if extracted_url:
            return None, extracted_url, None

        preview_text = full_text[:240].replace("\n", "\\n") if full_text else ""
        preview_raw = raw_content[:240]
        logger.warning(
            f"[{media_type}] 未提取到媒体: stream={is_streaming}, "
            f"text_preview={preview_text}, raw_preview={preview_raw}"
        )
        return None, None, "未能从响应中提取媒体内容"

    def _parse_json_response(self, data: dict) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        """解析 JSON 响应，返回 (url, base64, text)"""
        url = None
        b64 = None
        text = None

        # OpenAI 图像生成格式: {"data": [{"url": "..."} or {"b64_json": "..."}]}
        if "data" in data and isinstance(data["data"], list):
            for item in data["data"]:
                if isinstance(item, dict):
                    if item.get("url"):
                        url = item["url"]
                    if item.get("b64_json"):
                        b64 = item["b64_json"]
                    if item.get("revised_prompt"):
                        text = item["revised_prompt"]

        # Chat Completions 格式
        if "choices" in data:
            for choice in data.get("choices", []):
                msg = choice.get("message") or choice.get("delta") or {}
                content = msg.get("content")
                if content:
                    if isinstance(content, str):
                        text = content
                        extracted_url = self._extract_url_from_text(content)
                        if extracted_url:
                            url = extracted_url
                        else:
                            extracted_b64 = self._extract_base64_from_text(content)
                            if extracted_b64:
                                b64 = extracted_b64
                    elif isinstance(content, list):
                        for part in content:
                            if isinstance(part, dict):
                                if part.get("type") == "image_url":
                                    img_url = part.get("image_url", {}).get("url", "")
                                    if img_url.startswith("data:"):
                                        b64 = self._extract_base64_from_data_uri(img_url)
                                    else:
                                        url = img_url
                                elif part.get("type") == "text":
                                    text = part.get("text", "")

        # 直接字段
        for key in ("url", "image_url", "video_url", "media_url", "file_url"):
            if data.get(key):
                url = data[key]
                break

        for key in ("b64_json", "base64", "image_base64", "data"):
            val = data.get(key)
            if val and isinstance(val, str) and self._is_base64(val):
                b64 = val
                break

        for key in ("content", "text", "result", "output", "message"):
            val = data.get(key)
            if val and isinstance(val, str):
                text = val
                break

        return url, b64, text

    def _extract_media_from_chunk(self, chunk: dict) -> Tuple[Optional[str], Optional[str]]:
        """从流式块中提取媒体 URL 或 Base64"""
        url = None
        b64 = None

        # 递归搜索
        def search(obj):
            nonlocal url, b64
            if isinstance(obj, dict):
                for k, v in obj.items():
                    if k in ("url", "image_url", "video_url") and isinstance(v, str):
                        if v.startswith("data:"):
                            b64 = self._extract_base64_from_data_uri(v)
                        elif v.startswith("http"):
                            url = v
                    elif k in ("b64_json", "base64") and isinstance(v, str):
                        b64 = v
                    else:
                        search(v)
            elif isinstance(obj, list):
                for item in obj:
                    search(item)

        search(chunk)
        return url, b64

    @staticmethod
    def _extract_text_content(chunk: dict) -> Optional[str]:
        """从响应块中提取文本内容"""
        if chunk.get("choices"):
            choice = chunk["choices"][0]
            delta = choice.get("delta") or choice.get("message") or {}
            content = delta.get("content", "")
            if isinstance(content, list):
                return "".join(
                    str(c.get("text", "")) if isinstance(c, dict) else str(c)
                    for c in content
                )
            return str(content) if content else ""
        for key in ("content", "text", "result", "output"):
            val = chunk.get(key)
            if val:
                if isinstance(val, str):
                    return val
                if isinstance(val, list):
                    return "".join(
                        str(c.get("text", "")) if isinstance(c, dict) else str(c)
                        for c in val
                    )
        return None

    @staticmethod
    def _extract_url_from_text(text: str) -> Optional[str]:
        """从文本中提取媒体 URL"""
        if not text:
            return None
        text = text.strip()

        if text.startswith(("http://", "https://")):
            return text.split()[0].rstrip('.,;!?)\'\"')

        patterns = [
            r'<(?:video|source|img)[^>]*src=["\']([^"\']+)["\']',
            r'(https?://[^\s<>"\')\]\\]+\.(?:mp4|webm|mov|avi|mkv|png|jpg|jpeg|gif|webp|bmp)(?:[?#][^\s<>"\')\]\\]*)?)',
            r'!\[[^\]]*\]\(([^)]+)\)',
            r'"url"\s*:\s*"(https?://[^"]+)"',
            r'(https?://[^\s<>"\')\]\\]+)',
        ]
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                url = match.group(1)
                if url.startswith(("http://", "https://")):
                    return url
        return None

    @staticmethod
    def _extract_base64_from_text(text: str) -> Optional[str]:
        """从文本中提取 Base64 数据"""
        if not text:
            return None

        # data URI 格式
        match = re.search(r'data:[^;]+;base64,([A-Za-z0-9+/=]+)', text)
        if match:
            return match.group(1)

        # 纯 base64 字符串（至少100字符，避免误判）
        match = re.search(r'([A-Za-z0-9+/]{100,}={0,2})', text)
        if match:
            return match.group(1)

        return None

    @staticmethod
    def _extract_base64_from_data_uri(data_uri: str) -> Optional[str]:
        """从 data URI 中提取 Base64"""
        if "base64," in data_uri:
            return data_uri.split("base64,", 1)[1]
        return None

    @staticmethod
    def _is_base64(s: str) -> bool:
        """检查字符串是否为有效的 Base64"""
        if not s or len(s) < 100:
            return False
        try:
            if re.match(r'^[A-Za-z0-9+/]+={0,2}$', s):
                base64.b64decode(s[:100])
                return True
        except Exception:
            pass
        return False

    # ==================== 媒体处理 ====================

    async def _download_media(self, url: str) -> Optional[bytes]:
        session = await self._ensure_session()
        parsed = urlparse(url)
        origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.scheme and parsed.netloc else ""

        api_key = str(self.conf.get("grok_api_key", "")).strip()
        common_headers = {
            "Accept": "*/*",
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        }
        if origin:
            common_headers["Referer"] = origin + "/"

        header_candidates = []
        if api_key:
            h = dict(common_headers)
            h["Authorization"] = f"Bearer {api_key}"
            header_candidates.append(("auth", h))
        header_candidates.append(("plain", dict(common_headers)))

        timeout = aiohttp.ClientTimeout(total=120)
        last_error = None

        for tag, headers in header_candidates:
            try:
                async with session.get(url, headers=headers, timeout=timeout) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    detail = (await resp.text())[:200]
                    last_error = f"{resp.status} {detail}"
                    logger.warning(f"媒体下载失败[{tag}]: status={resp.status}, url={url}, detail={detail}")
                    if resp.status != 403:
                        break
            except Exception as e:
                last_error = str(e)
                logger.warning(f"媒体下载异常[{tag}]: {e}, url={url}")

        logger.error(f"媒体下载失败: {last_error or 'unknown error'}, url='{url}'")
        return None

    async def _load_bytes(self, src: str) -> Optional[bytes]:
        if Path(src).is_file():
            try:
                async with aiofiles.open(src, 'rb') as f:
                    return await f.read()
            except Exception as e:
                logger.debug(f"读取本地文件失败 ({src[:50]}): {e}")
                return None
        elif src.startswith("http"):
            return await self._download_media(src)
        elif src.startswith("data:"):
            try:
                b64_data = self._extract_base64_from_data_uri(src)
                if b64_data:
                    return base64.b64decode(b64_data)
            except Exception as e:
                logger.debug(f"Data URI 解码失败: {e}")
                return None
        elif src.startswith("base64://"):
            try:
                return base64.b64decode(src[9:])
            except Exception as e:
                logger.debug(f"Base64解码失败: {e}")
                return None
        return None

    async def _download_and_cache_image(self, url: str) -> Optional[bytes]:
        """下载图片并缓存到本地，控制缓存数量"""
        payload = await self._download_media(url)
        if not payload:
            return None
        try:
            self.cache_image_dir.mkdir(exist_ok=True, parents=True)
            filename = self._guess_filename_from_source(url, "cache_image.png")
            suffix = Path(filename).suffix or ".png"
            path = (self.cache_image_dir / f"cache_{int(time.time())}_{uuid.uuid4().hex[:8]}{suffix}").resolve()
            async with aiofiles.open(path, 'wb') as f:
                await f.write(payload)
        except Exception as e:
            logger.warning(f"缓存图片失败: {e}")
            return payload

        keep = self._get_media_file_limit("image", persistent=True)
        self._cleanup_directory_to_limit(self.cache_image_dir, keep)
        return payload

    @staticmethod
    def _unwrap_action_response(payload: Any) -> Dict[str, Any]:
        if not isinstance(payload, dict):
            return {}
        data = payload.get("data")
        if isinstance(data, dict):
            return data
        return payload

    @staticmethod
    def _extract_text_from_segment(seg: Any) -> str:
        containers: List[Any] = [seg]
        data = seg.get("data") if isinstance(seg, dict) else getattr(seg, "data", None)
        if isinstance(data, dict):
            containers.append(data)

        for container in containers:
            if isinstance(container, dict):
                for key in ("text", "content"):
                    value = container.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
            else:
                for key in ("text", "content"):
                    value = getattr(container, key, None)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
        return ""

    def _get_message_text_without_reply(self, event: AstrMessageEvent) -> str:
        message_list = getattr(getattr(event, "message_obj", None), "message", None) or []
        parts: List[str] = []
        for seg in message_list:
            if self._is_segment_type(seg, "Reply"):
                continue
            piece = self._extract_text_from_segment(seg)
            if piece:
                parts.append(piece)
        text = " ".join(parts).strip()
        if text:
            return text
        return (getattr(event, "message_str", "") or "").strip()

    def _extract_image_refs_and_forward_ids(
        self,
        payload: Any,
        depth: int = 0,
        max_depth: int = 6,
    ) -> Tuple[List[str], List[str]]:
        if payload is None or depth > max_depth:
            return [], []

        image_refs: List[str] = []
        forward_ids: List[str] = []
        nested_keys = (
            "message",
            "messages",
            "chain",
            "content",
            "nodes",
            "origin",
            "reply",
            "reference",
            "quote",
            "source",
            "source_message",
            "source_content",
        )

        if isinstance(payload, (list, tuple)):
            for item in payload:
                refs, ids = self._extract_image_refs_and_forward_ids(item, depth + 1, max_depth)
                image_refs.extend(refs)
                forward_ids.extend(ids)
            return self._dedupe_strings(image_refs), self._dedupe_strings(forward_ids)

        if isinstance(payload, dict):
            seg_type = str(payload.get("type", "") or "").lower()
            if seg_type == "image":
                image_refs.extend(self._extract_segment_sources(payload))

            for key, value in payload.items():
                key_lower = str(key or "").strip().lower()
                if value is None or isinstance(value, (dict, list, tuple)):
                    continue
                if (
                    key_lower in {"reply_id", "quote_id", "reference_id", "source_id", "source_message_id", "source_msg_id"}
                    or (
                        seg_type in ("reply", "quote", "reference")
                        and key_lower in {"id", "message_id"}
                    )
                ):
                    scalar = str(value).strip()
                    if scalar:
                        forward_ids.append(scalar)

            data = payload.get("data")
            if isinstance(data, dict):
                if seg_type == "image":
                    image_refs.extend(self._extract_segment_sources(data))
                if seg_type in ("forward", "forward_msg", "nodes", "reply", "quote", "reference"):
                    for key in ("id", "message_id"):
                        value = data.get(key)
                        if value is not None and str(value).strip():
                            forward_ids.append(str(value).strip())
                for key in nested_keys:
                    refs, ids = self._extract_image_refs_and_forward_ids(data.get(key), depth + 1, max_depth)
                    image_refs.extend(refs)
                    forward_ids.extend(ids)

            for key in nested_keys:
                refs, ids = self._extract_image_refs_and_forward_ids(payload.get(key), depth + 1, max_depth)
                image_refs.extend(refs)
                forward_ids.extend(ids)

            return self._dedupe_strings(image_refs), self._dedupe_strings(forward_ids)

        if self._is_segment_type(payload, "Image"):
            image_refs.extend(self._extract_segment_sources(payload))

        seg_type_name = self._segment_type_name(payload)
        if seg_type_name in ("forward", "forward_msg", "nodes", "reply", "quote", "reference"):
            for key in ("id", "message_id"):
                value = self._get_segment_attr(payload, key)
                if value is not None and str(value).strip():
                    forward_ids.append(str(value).strip())

        for attr in nested_keys:
            refs, ids = self._extract_image_refs_and_forward_ids(
                self._get_segment_attr(payload, attr),
                depth + 1,
                max_depth,
            )
            image_refs.extend(refs)
            forward_ids.extend(ids)

        if hasattr(payload, "__dict__"):
            refs, ids = self._extract_image_refs_and_forward_ids(
                getattr(payload, "__dict__", None),
                depth + 1,
                max_depth,
            )
            image_refs.extend(refs)
            forward_ids.extend(ids)

        return self._dedupe_strings(image_refs), self._dedupe_strings(forward_ids)

    def _extract_reply_and_image_refs_from_text(self, text: str) -> Tuple[List[str], List[str]]:
        raw = str(text or "").strip()
        if not raw:
            return [], []

        reply_ids = re.findall(r"\[CQ:reply,[^\]]*\bid=([^,\]]+)", raw, flags=re.IGNORECASE)
        image_refs: List[str] = []
        for key in ("url", "file", "file_id", "src"):
            image_refs.extend(
                re.findall(
                    rf"\[CQ:image,[^\]]*\b{key}=([^,\]]+)",
                    raw,
                    flags=re.IGNORECASE,
                )
            )
        return self._dedupe_strings(reply_ids), self._dedupe_strings(image_refs)

    def _collect_reply_hints_from_event(self, event: AstrMessageEvent) -> Tuple[List[str], List[str], List[str]]:
        reply_ids: List[str] = []
        image_refs: List[str] = []
        debug_sources: List[str] = []

        msg_obj = getattr(event, "message_obj", None)
        text_candidates = [
            getattr(event, "message_str", ""),
            getattr(event, "raw_message", ""),
            getattr(msg_obj, "raw_message", "") if msg_obj is not None else "",
            getattr(msg_obj, "message_str", "") if msg_obj is not None else "",
            getattr(msg_obj, "original_message", "") if msg_obj is not None else "",
        ]
        for idx, candidate in enumerate(text_candidates):
            ids, refs = self._extract_reply_and_image_refs_from_text(candidate)
            if ids or refs:
                debug_sources.append(f"text[{idx}]")
            reply_ids.extend(ids)
            image_refs.extend(refs)

        for label, root in (
            ("message_obj", msg_obj),
            ("raw_message", getattr(msg_obj, "raw_message", None) if msg_obj is not None else None),
            ("event_dict", getattr(event, "__dict__", None)),
        ):
            refs, ids = self._extract_image_refs_and_forward_ids(root)
            if refs or ids:
                debug_sources.append(label)
                image_refs.extend(refs)
                reply_ids.extend(ids)

        return (
            self._dedupe_strings(reply_ids),
            self._dedupe_strings(image_refs),
            self._dedupe_strings(debug_sources),
        )

    async def _collect_reply_hints_from_current_message(
        self,
        event: AstrMessageEvent,
    ) -> Tuple[List[str], List[str], bool]:
        msg_obj = getattr(event, "message_obj", None)
        message_id = (
            getattr(msg_obj, "message_id", None)
            or getattr(event, "message_id", None)
            or self._get_segment_field(getattr(msg_obj, "raw_message", None), "message_id", "id")
        )
        if message_id is None or not str(message_id).strip():
            return [], [], False

        payload = await self._get_bot_message_payload(event, "get_msg", message_id)
        if not payload:
            return [], [], False

        image_refs, reply_ids = self._extract_image_refs_and_forward_ids(payload)
        if not reply_ids and not image_refs:
            logger.info(
                f"[引用图] current_message_lookup_summary: "
                f"message_id={message_id}, segments={self._summarize_message_like_payload(payload)}"
            )
        return (
            self._dedupe_strings(reply_ids),
            self._dedupe_strings(image_refs),
            True,
        )

    def _coerce_reply_component_for_quote_parser(self, seg: Any) -> Optional[Any]:
        if not self._is_segment_type(seg, "Reply"):
            return None

        reply_cls = getattr(Comp, "Reply", None)
        if reply_cls is not None:
            try:
                if isinstance(seg, reply_cls):
                    return seg
            except Exception:
                pass

        reply_id = self._get_segment_field(seg, "id", "message_id")
        if reply_id is None or not str(reply_id).strip():
            return None

        if reply_cls is None:
            return None

        kwargs: Dict[str, Any] = {"id": str(reply_id).strip()}
        for src_key, dst_key in (
            ("message_str", "message_str"),
            ("sender_nickname", "sender_nickname"),
            ("sender_id", "sender_id"),
            ("time", "time"),
            ("text", "text"),
            ("qq", "qq"),
        ):
            value = self._get_segment_attr(seg, src_key)
            if value is not None and str(value).strip():
                kwargs[dst_key] = value

        try:
            return reply_cls(**kwargs)
        except Exception:
            return None

    def _collect_reply_components_for_quote_parser(self, event: AstrMessageEvent) -> List[Any]:
        reply_components: List[Any] = []
        seen = set()

        message_list = getattr(getattr(event, "message_obj", None), "message", None) or []
        for seg in message_list:
            reply_component = self._coerce_reply_component_for_quote_parser(seg)
            if not reply_component:
                continue
            reply_id = str(getattr(reply_component, "id", "") or "").strip()
            dedupe_key = reply_id or f"obj:{id(reply_component)}"
            if dedupe_key in seen:
                continue
            seen.add(dedupe_key)
            reply_components.append(reply_component)

        reply_cls = getattr(Comp, "Reply", None)
        if reply_cls is not None:
            reply_ids, _, _ = self._collect_reply_hints_from_event(event)
            for reply_id in reply_ids:
                cleaned = str(reply_id or "").strip()
                if not cleaned or cleaned in seen:
                    continue
                try:
                    reply_components.append(reply_cls(id=cleaned))
                    seen.add(cleaned)
                except Exception:
                    continue

        return reply_components

    async def _extract_quoted_image_refs_via_astrbot(self, event: AstrMessageEvent) -> List[str]:
        if not callable(astrbot_extract_quoted_message_images):
            return []

        reply_components = self._collect_reply_components_for_quote_parser(event)
        collected_refs: List[str] = []

        try:
            if reply_components:
                for reply_component in reply_components:
                    try:
                        refs = await astrbot_extract_quoted_message_images(
                            event,
                            reply_component=reply_component,
                        )
                    except TypeError:
                        refs = await astrbot_extract_quoted_message_images(
                            event,
                            reply_component,
                        )
                    if isinstance(refs, list):
                        collected_refs.extend(
                            str(item).strip()
                            for item in refs
                            if isinstance(item, str) and str(item).strip()
                        )
            else:
                refs = await astrbot_extract_quoted_message_images(event)
                if isinstance(refs, list):
                    collected_refs.extend(
                        str(item).strip()
                        for item in refs
                        if isinstance(item, str) and str(item).strip()
                    )
        except Exception as e:
            logger.debug(f"[引用图] AstrBot 官方解析失败: {e}")
            return []

        return self._dedupe_strings(collected_refs)

    async def _load_image_bytes_from_refs(
        self,
        event: AstrMessageEvent,
        image_refs: List[str],
        max_count: int = 1,
    ) -> List[bytes]:
        images: List[bytes] = []
        for image_ref in self._dedupe_strings(image_refs):
            source = image_ref
            if not (
                source.startswith(("http://", "https://", "base64://", "data:"))
                or Path(source).is_file()
            ):
                resolved = await self._resolve_image_ref_via_bot_api(event, source)
                if not resolved:
                    continue
                source = resolved

            payload = await self._load_bytes(source)
            if not payload and source.startswith("http"):
                payload = await self._download_and_cache_image(source)
            if not payload:
                continue
            if any(existing == payload for existing in images):
                continue
            images.append(payload)
            if len(images) >= max_count:
                break

        return images

    def _event_has_reply_context(self, event: AstrMessageEvent) -> bool:
        if self._collect_reply_components_for_quote_parser(event):
            return True
        reply_ids, _, _ = self._collect_reply_hints_from_event(event)
        return bool(reply_ids)

    def _summarize_message_like_payload(self, payload: Any, max_items: int = 8) -> List[str]:
        message = None
        if isinstance(payload, dict):
            message = payload.get("message") or payload.get("messages")
        else:
            message = getattr(payload, "message", None) or getattr(payload, "messages", None)

        if not isinstance(message, list):
            return []

        preview: List[str] = []
        for seg in message[:max_items]:
            try:
                if isinstance(seg, dict):
                    seg_type = str(seg.get("type", "") or "")
                    data = seg.get("data", {})
                    keys = sorted(list(data.keys()))[:8] if isinstance(data, dict) else []
                    preview.append(f"{seg_type}:{keys}")
                else:
                    seg_type = self._segment_type_name(seg)
                    attrs = sorted(list(getattr(seg, "__dict__", {}).keys()))[:8]
                    preview.append(f"{seg_type}:{attrs}")
            except Exception as e:
                preview.append(f"<summary_error:{e}>")
        return preview

    async def _call_bot_action(
        self,
        event: AstrMessageEvent,
        action: str,
        params_list: List[Dict[str, Any]],
        unwrap_data: bool = True,
    ) -> Optional[Dict[str, Any]]:
        bot = getattr(event, "bot", None)
        api = getattr(bot, "api", None)
        call_action = getattr(api, "call_action", None)
        if not callable(call_action):
            return None

        for params in params_list:
            try:
                result = call_action(action, **params)
                if hasattr(result, "__await__"):
                    result = await result
                if isinstance(result, dict):
                    return self._unwrap_action_response(result) if unwrap_data else result
            except Exception as e:
                logger.debug(
                    f"调用平台动作失败: action={action}, params={params}, error={e}"
                )
        return None

    async def _get_bot_message_payload(
        self,
        event: AstrMessageEvent,
        action: str,
        message_id: Any,
    ) -> Optional[Dict[str, Any]]:
        message_id_str = str(message_id or "").strip()
        if not message_id_str:
            return None

        params_list: List[Dict[str, Any]] = [
            {"message_id": message_id_str},
            {"id": message_id_str},
        ]
        if message_id_str.isdigit():
            int_id = int(message_id_str)
            params_list.extend([
                {"message_id": int_id},
                {"id": int_id},
            ])

        return await self._call_bot_action(event, action, params_list, unwrap_data=True)

    async def _resolve_image_ref_via_bot_api(
        self,
        event: AstrMessageEvent,
        image_ref: str,
    ) -> Optional[str]:
        image_ref = str(image_ref or "").strip()
        if not image_ref:
            return None

        candidates = [image_ref]
        base_name, ext = os.path.splitext(image_ref)
        if ext and base_name:
            candidates.append(base_name)

        params_to_try: List[Tuple[str, Dict[str, Any]]] = []
        for candidate in self._dedupe_strings(candidates):
            params_to_try.extend([
                ("get_image", {"file": candidate}),
                ("get_image", {"file_id": candidate}),
                ("get_image", {"id": candidate}),
                ("get_image", {"image": candidate}),
                ("get_file", {"file_id": candidate}),
                ("get_file", {"file": candidate}),
            ])

        try:
            group_id = event.get_group_id()
        except Exception:
            group_id = ""

        if group_id:
            group_value: Any = int(group_id) if str(group_id).isdigit() else group_id
            for candidate in self._dedupe_strings(candidates):
                params_to_try.append(
                    ("get_group_file_url", {"group_id": group_value, "file_id": candidate})
                )

        for candidate in self._dedupe_strings(candidates):
            params_to_try.append(("get_private_file_url", {"file_id": candidate}))

        for action, params in params_to_try:
            payload = await self._call_bot_action(event, action, [params], unwrap_data=True)
            if not isinstance(payload, dict):
                continue
            for key in ("url", "file", "path", "src"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()

        return None

    async def _get_reply_image_bytes_from_event(
        self,
        event: AstrMessageEvent,
        max_count: int = 1,
    ) -> List[bytes]:
        if max_count <= 0:
            return []

        message_list = getattr(getattr(event, "message_obj", None), "message", None) or []
        reply_segments = self._collect_reply_components_for_quote_parser(event)

        official_image_refs = await self._extract_quoted_image_refs_via_astrbot(event)
        official_images = await self._load_image_bytes_from_refs(
            event,
            official_image_refs,
            max_count=max_count,
        )
        if official_image_refs:
            logger.info(
                f"[引用图] astrbot_refs={len(official_image_refs)}, loaded={len(official_images)}, "
                f"reply_segments={len(reply_segments)}"
            )
        if len(official_images) >= max_count:
            return official_images[:max_count]

        image_refs: List[str] = []
        pending_forward_ids: List[str] = []
        reply_ids_from_fallback: List[str] = []
        fallback_debug_sources: List[str] = []

        if not reply_segments:
            reply_ids_from_fallback, fallback_image_refs, fallback_debug_sources = (
                self._collect_reply_hints_from_event(event)
            )
            image_refs.extend(fallback_image_refs)
            if not reply_ids_from_fallback and not image_refs:
                (
                    current_reply_ids,
                    current_image_refs,
                    current_lookup_loaded,
                ) = await self._collect_reply_hints_from_current_message(event)
                if current_lookup_loaded:
                    fallback_debug_sources.append("current_message_lookup")
                    reply_ids_from_fallback.extend(current_reply_ids)
                    image_refs.extend(current_image_refs)

        for reply in reply_segments:
            for attr in ("chain", "message", "messages", "origin", "content", "nodes"):
                refs, forward_ids = self._extract_image_refs_and_forward_ids(
                    self._get_segment_attr(reply, attr)
                )
                image_refs.extend(refs)
                pending_forward_ids.extend(forward_ids)

            reply_id = self._get_segment_field(reply, "id", "message_id")
            if reply_id is None or not str(reply_id).strip():
                continue

            payload = await self._get_bot_message_payload(event, "get_msg", reply_id)
            if not payload:
                continue

            refs, forward_ids = self._extract_image_refs_and_forward_ids(payload)
            image_refs.extend(refs)
            pending_forward_ids.extend(forward_ids)

        for reply_id in reply_ids_from_fallback:
            payload = await self._get_bot_message_payload(event, "get_msg", reply_id)
            if not payload:
                continue
            refs, forward_ids = self._extract_image_refs_and_forward_ids(payload)
            image_refs.extend(refs)
            pending_forward_ids.extend(forward_ids)

        seen_forward = set()
        queue = self._dedupe_strings(pending_forward_ids)
        hops = 0
        while queue and hops < 5:
            forward_id = queue.pop(0)
            if not forward_id or forward_id in seen_forward:
                continue
            seen_forward.add(forward_id)
            hops += 1

            payload = await self._get_bot_message_payload(event, "get_forward_msg", forward_id)
            if not payload:
                continue

            refs, forward_ids = self._extract_image_refs_and_forward_ids(payload)
            image_refs.extend(refs)
            for nested_id in forward_ids:
                if nested_id not in seen_forward:
                    queue.append(nested_id)

        images: List[bytes] = list(official_images)
        remaining_slots = max_count - len(images)
        if remaining_slots > 0:
            fallback_images = await self._load_image_bytes_from_refs(
                event,
                image_refs,
                max_count=remaining_slots,
            )
            for payload in fallback_images:
                if any(existing == payload for existing in images):
                    continue
                images.append(payload)
                if len(images) >= max_count:
                    break

        if reply_segments:
            logger.info(
                f"[引用图] reply_segments={len(reply_segments)}, image_refs={len(self._dedupe_strings(image_refs))}, loaded={len(images)}"
            )
        elif fallback_debug_sources:
            logger.info(
                f"[引用图] fallback_sources={fallback_debug_sources}, reply_ids={len(reply_ids_from_fallback)}, "
                f"image_refs={len(self._dedupe_strings(image_refs))}, loaded={len(images)}"
            )
        elif not images:
            msg_obj = getattr(event, "message_obj", None)
            top_level_keys = sorted(list(getattr(msg_obj, "__dict__", {}).keys())) if msg_obj is not None and hasattr(msg_obj, "__dict__") else []
            segment_preview: List[str] = []
            message_list = getattr(msg_obj, "message", None) or []
            raw_message_preview = self._summarize_message_like_payload(
                getattr(msg_obj, "raw_message", None) if msg_obj is not None else None
            )
            for seg in list(message_list)[:6]:
                try:
                    data = getattr(seg, "data", None)
                    if isinstance(seg, dict):
                        seg_repr = {
                            "type": seg.get("type"),
                            "keys": sorted(list(seg.keys()))[:12],
                            "data": data if isinstance(data, (str, int, float, bool, type(None))) else (
                                {k: data.get(k) for k in list(data.keys())[:8]} if isinstance(data, dict) else str(data)[:160]
                            ),
                        }
                    else:
                        seg_repr = {
                            "cls": seg.__class__.__name__,
                            "type": getattr(seg, "type", None),
                            "attrs": sorted(list(getattr(seg, "__dict__", {}).keys()))[:12],
                            "data": data if isinstance(data, (str, int, float, bool, type(None))) else (
                                {k: data.get(k) for k in list(data.keys())[:8]} if isinstance(data, dict) else str(data)[:160]
                            ),
                        }
                    segment_preview.append(str(seg_repr)[:240])
                except Exception as e:
                    segment_preview.append(f"<segment_preview_error {e}>")
            logger.warning(
                f"[引用图] 未发现可用回复线索: message_obj_keys={top_level_keys}, "
                f"message_str={(getattr(event, 'message_str', '') or '')[:120]}, "
                f"segments={segment_preview}, raw_segments={raw_message_preview}"
            )
        return images

    def _iter_event_segments(self, event: AstrMessageEvent) -> List[Any]:
        """展开消息链与引用链，返回统一的消息段列表"""
        message_list = getattr(getattr(event, "message_obj", None), "message", None) or []
        segments: List[Any] = []
        for seg in message_list:
            if self._is_segment_type(seg, "Reply") and getattr(seg, "chain", None):
                for inner in seg.chain:
                    segments.append(inner)
            else:
                segments.append(seg)
        return segments

    async def _load_segment_payload(self, seg: Any) -> Tuple[Optional[bytes], Optional[str]]:
        """从消息段中读取媒体数据，返回 (bytes, source)"""
        direct_data = getattr(seg, "data", None)
        if isinstance(direct_data, (bytes, bytearray)) and direct_data:
            return bytes(direct_data), None

        for src in self._extract_segment_sources(seg):
            payload = await self._load_bytes(src)
            if not payload and src.startswith("http"):
                payload = await self._download_and_cache_image(src)
            if payload:
                return payload, src
        return None, None

    async def _get_direct_images_from_event(
        self,
        event: AstrMessageEvent,
        max_count: int = 1,
    ) -> List[bytes]:
        images: List[bytes] = []
        if max_count <= 0:
            return images

        message_list = getattr(getattr(event, "message_obj", None), "message", None) or []
        for seg in message_list:
            if not self._is_segment_type(seg, "Image"):
                continue
            payload, _ = await self._load_segment_payload(seg)
            if not payload:
                continue
            if any(existing == payload for existing in images):
                continue
            images.append(payload)
            if len(images) >= max_count:
                break

        return images

    async def _get_images_from_event(
        self,
        event: AstrMessageEvent,
        max_count: int = 1,
    ) -> List[bytes]:
        images: List[bytes] = []
        if max_count <= 0:
            return images

        for seg in self._iter_event_segments(event):
            if not self._is_segment_type(seg, "Image"):
                continue
            payload, _ = await self._load_segment_payload(seg)
            if payload:
                images.append(payload)
                if len(images) >= max_count:
                    break
        return images

    async def _get_image_from_event(self, event: AstrMessageEvent) -> Optional[bytes]:
        images = await self._get_images_from_event(event, max_count=1)
        if images:
            return images[0]
        return None

    async def _collect_multimodal_inputs(self, event: AstrMessageEvent) -> Dict[str, Any]:
        images = await self._get_images_from_event(event, max_count=1)
        image_bytes = images[0] if images else None

        audio_inputs: List[Dict[str, str]] = []
        file_inputs: List[Dict[str, str]] = []

        for seg in self._iter_event_segments(event):
            is_audio = (
                self._is_segment_type(seg, "Record")
                or self._is_segment_type(seg, "Audio")
                or self._is_segment_type(seg, "Voice")
            )
            is_file = self._is_segment_type(seg, "File")

            if not is_audio and not is_file:
                continue

            sources = self._extract_segment_sources(seg)
            payload, source = await self._load_segment_payload(seg)
            if not source and sources:
                source = sources[0]
            if not payload:
                if is_file:
                    file_url = next((s for s in sources if s.startswith("http")), "")
                    if file_url:
                        file_inputs.append(
                            {
                                "filename": self._guess_filename_from_source(
                                    file_url,
                                    "upload.bin",
                                ),
                                "url": file_url,
                            }
                        )
                continue

            if is_audio:
                audio_inputs.append(
                    {
                        "format": self._guess_audio_format_from_source(source),
                        "data": base64.b64encode(payload).decode("utf-8"),
                    }
                )
                continue

            filename = self._guess_filename_from_source(source, "upload.bin")
            mime_type = self._guess_mime_type_from_source(
                source,
                "application/octet-stream",
            )
            file_inputs.append(
                {
                    "filename": filename,
                    "data": (
                        f"data:{mime_type};base64,"
                        f"{base64.b64encode(payload).decode('utf-8')}"
                    ),
                }
            )

        return {
            "image_bytes": image_bytes,
            "audio_inputs": audio_inputs,
            "file_inputs": file_inputs,
        }



    @staticmethod
    def _normalize_id_list(values: Any) -> List[str]:
        """将配置中的 ID 列表标准化为字符串列表"""
        if values is None:
            return []
        if not isinstance(values, (list, tuple, set)):
            values = [values]
        result: List[str] = []
        seen = set()
        for v in values:
            if v is None:
                continue
            item = str(v).strip()
            if not item or item in seen:
                continue
            seen.add(item)
            result.append(item)
        return result

    @staticmethod
    def _extract_session_context(event: AstrMessageEvent) -> Tuple[str, str, bool]:
        """提取 user_id / group_id / 是否群聊"""
        user_id = ""
        group_id = ""
        is_group = False

        try:
            sid = str(getattr(event, "unified_msg_origin", "") or "")
            if sid:
                if "group" in sid.lower() or "guild" in sid.lower():
                    is_group = True
                nums = re.findall(r"\d+", sid)
                if nums:
                    if is_group:
                        group_id = nums[0]
                        if len(nums) > 1:
                            user_id = nums[-1]
                    else:
                        user_id = nums[-1]
        except Exception:
            pass

        sender = getattr(event, "sender", None)
        if sender is not None and not user_id:
            user_id = str(getattr(sender, "user_id", "") or getattr(sender, "id", "") or "").strip()

        msg_obj = getattr(event, "message_obj", None)
        if msg_obj is not None:
            if not user_id:
                user_id = str(getattr(msg_obj, "user_id", "") or getattr(msg_obj, "sender_id", "") or "").strip()
            if not group_id:
                group_id = str(getattr(msg_obj, "group_id", "") or getattr(msg_obj, "channel_id", "") or "").strip()
            if group_id:
                is_group = True

        if not user_id:
            try:
                user_id = str(event.get_sender_id() or "").strip()
            except Exception:
                pass

        return user_id, group_id, is_group

    def _get_recent_session_key(self, event: AstrMessageEvent) -> str:
        session_key = str(getattr(event, "unified_msg_origin", "") or "").strip()
        if session_key:
            return session_key
        msg_obj = getattr(event, "message_obj", None)
        session_key = str(getattr(msg_obj, "session_id", "") or "").strip()
        if session_key:
            return session_key
        user_id, group_id, is_group = self._extract_session_context(event)
        if is_group and group_id:
            return f"group:{group_id}"
        if user_id:
            return f"user:{user_id}"
        return ""

    def _prune_recent_session_images(self, session_key: Optional[str] = None) -> None:
        now = time.time()
        session_keys = [session_key] if session_key else list(self._recent_session_images.keys())
        for key in session_keys:
            if not key:
                continue
            bucket = self._recent_session_images.get(key, [])
            bucket = [
                item for item in bucket
                if isinstance(item, dict)
                and isinstance(item.get("bytes"), bytes)
                and now - float(item.get("ts", 0) or 0) <= self.RECENT_SESSION_IMAGE_TTL_SECONDS
            ]
            if not bucket:
                self._recent_session_images.pop(key, None)
                continue
            self._recent_session_images[key] = bucket[: self.RECENT_SESSION_IMAGE_LIMIT]

    def _remember_recent_session_image(
        self,
        event: AstrMessageEvent,
        image_bytes: bytes,
        source: str,
    ) -> None:
        if not image_bytes:
            return
        session_key = self._get_recent_session_key(event)
        if not session_key:
            return

        self._prune_recent_session_images(session_key)
        digest = hashlib.sha256(image_bytes).hexdigest()
        sender_id = ""
        try:
            sender_id = str(event.get_sender_id() or "").strip()
        except Exception:
            sender_id = ""

        bucket = self._recent_session_images.setdefault(session_key, [])
        bucket = [item for item in bucket if item.get("hash") != digest]
        bucket.insert(
            0,
            {
                "hash": digest,
                "bytes": image_bytes,
                "ts": int(time.time()),
                "source": str(source or "").strip() or "unknown",
                "sender_id": sender_id,
            },
        )
        self._recent_session_images[session_key] = bucket[: self.RECENT_SESSION_IMAGE_LIMIT]

    def _looks_like_reference_image_prompt(self, prompt: str) -> bool:
        text = str(prompt or "").strip()
        if not text:
            return False
        edit_keywords = (
            "改", "修改", "调成", "变成", "换成", "替换", "重绘", "编辑",
            "背景", "天空", "衣服", "头发", "颜色", "黑色", "白色",
            "原图", "参考图", "这张", "这个", "它", "他", "她", "保留"
        )
        if any(keyword in text for keyword in edit_keywords):
            return True
        return bool(re.search(r"^(把|将).{0,24}(改|变|换|调)", text))

    def _looks_like_reference_video_prompt(self, prompt: str) -> bool:
        text = str(prompt or "").strip()
        if not text:
            return False
        motion_keywords = (
            "动起来", "眨眼", "微笑", "转头", "呼吸", "风动", "风吹",
            "摇摆", "飘动", "走动", "镜头", "运镜", "推进", "拉远"
        )
        if not any(keyword in text for keyword in motion_keywords):
            return False
        reference_keywords = (
            "让他", "让她", "让它", "让这", "让这个", "让这张",
            "原图", "参考图", "这张图", "这个人物", "这个角色",
            "保持原图", "保留原图", "基于原图"
        )
        return any(keyword in text for keyword in reference_keywords)

    def _get_recent_session_image(
        self,
        event: AstrMessageEvent,
    ) -> Optional[bytes]:
        session_key = self._get_recent_session_key(event)
        if not session_key:
            return None
        self._prune_recent_session_images(session_key)
        bucket = self._recent_session_images.get(session_key, [])
        if not bucket:
            return None
        payload = bucket[0].get("bytes")
        if isinstance(payload, bytes) and payload:
            return payload
        return None

    def _try_recent_session_image_fallback(
        self,
        event: AstrMessageEvent,
        prompt: str,
        media_kind: str,
    ) -> Optional[bytes]:
        if media_kind == "image":
            should_use = self._looks_like_reference_image_prompt(prompt)
        else:
            should_use = self._looks_like_reference_video_prompt(prompt)
        if not should_use:
            return None

        recent_image = self._get_recent_session_image(event)
        if recent_image:
            logger.info(
                f"[引用图] 上游未提供 reply 元数据，改用最近会话图片兜底: "
                f"kind={media_kind}, bytes={len(recent_image)}"
            )
        return recent_image

    async def _check_permissions(self, event: AstrMessageEvent) -> Tuple[bool, str]:
        """检查当前会话是否允许使用插件"""
        user_id, group_id, is_group = self._extract_session_context(event)

        user_whitelist = self._normalize_id_list(self.conf.get("user_whitelist", []))
        user_blacklist = set(self._normalize_id_list(self.conf.get("user_blacklist", [])))
        group_whitelist = self._normalize_id_list(self.conf.get("group_whitelist", []))
        group_blacklist = set(self._normalize_id_list(self.conf.get("group_blacklist", [])))

        if user_id and user_id in user_blacklist:
            return False, "user_blacklist"

        if user_whitelist:
            uw = set(user_whitelist)
            if not user_id or user_id not in uw:
                return False, "user_whitelist"

        if is_group:
            if group_id and group_id in group_blacklist:
                return False, "group_blacklist"
            if group_whitelist:
                gw = set(group_whitelist)
                if not group_id or group_id not in gw:
                    return False, "group_whitelist"

        return True, "ok"



    async def _save_and_send_media(
        self,
        event: AstrMessageEvent,
        source: str,
        media_bytes: bytes,
        media_type: str,
    ):
        """保存并发送媒体（image/video）"""
        save_media = bool(self.conf.get("save_media", False))
        suffix = ".png" if media_type == "image" else ".mp4"
        folder = self._get_media_directory(media_type, persistent=save_media)
        folder.mkdir(exist_ok=True, parents=True)

        filename = f"{int(time.time())}_{uuid.uuid4().hex[:8]}{suffix}"
        path = (folder / filename).resolve()

        try:
            async with aiofiles.open(path, 'wb') as f:
                await f.write(media_bytes)
            self._cleanup_directory_to_limit(
                folder,
                self._get_media_file_limit(media_type, persistent=save_media),
            )

            if media_type == "image":
                self._remember_recent_session_image(event, media_bytes, source="plugin_output")

            if media_type == "image":
                comp = Comp.Image.fromFileSystem(str(path))
            else:
                comp = Comp.Video.fromFileSystem(path=str(path), name=filename)
            yield event.chain_result([comp])
        except Exception as e:
            logger.error(f"发送{media_type}失败: {e}")
            yield event.plain_result(f"❌ {media_type}发送失败: {self._translate_error(str(e))}")
        finally:
            if not save_media:
                try:
                    await aiofiles.os.remove(path)
                except Exception:
                    pass

    async def _send_images_forward(
        self,
        event: AstrMessageEvent,
        images_data: List[Tuple[str, bytes]],
        failed_count: int = 0,
    ):
        """多图发送（兼容：逐张发送）"""
        sent = 0
        for src, img_bytes in images_data:
            async for result in self._save_and_send_media(event, src, img_bytes, "image"):
                yield result
            sent += 1

        if sent == 0:
            yield event.plain_result("❌ 图片发送失败")
        elif failed_count > 0:
            yield event.plain_result(f"⚠️ {failed_count}张图片下载失败，请到后台查看")



    @staticmethod
    def _extract_user_input_from_command(raw_input: str, aliases: List[str]) -> str:
        """从原始文本中剥离命令头，返回用户参数文本。兼容 # / ! 前缀。"""
        text = (raw_input or "").strip()
        if not text:
            return ""

        if text[0] in ('#', '/', '!', '！'):
            text = text[1:].strip()

        if not text:
            return ""

        parts = text.split(maxsplit=1)
        first = parts[0].strip().lower()
        alias_set = {a.strip().lower() for a in aliases if a and a.strip()}

        if first in alias_set:
            return parts[1].strip() if len(parts) > 1 else ""

        lowered = text.lower()
        for alias in sorted(alias_set, key=len, reverse=True):
            for prefix in ("#", "/", "!", "！", ""):
                token = f"{prefix}{alias}".lower()
                idx = lowered.find(token)
                if idx < 0:
                    continue
                if idx > 0 and not text[idx - 1].isspace():
                    continue
                return text[idx + len(token):].strip()

        # 某些平台可能已提前去掉命令词，直接把剩余文本当参数
        return text

    @staticmethod
    def _looks_like_command_text(raw_input: str, aliases: List[str]) -> bool:
        text = (raw_input or "").strip()
        if not text:
            return False

        first = text.split(maxsplit=1)[0].strip()
        first = first.lstrip("#/!？?").strip().lower()
        if not first:
            return False

        alias_set = {
            str(alias or "").strip().lstrip("#/!？?").lower()
            for alias in aliases
            if str(alias or "").strip()
        }
        return first in alias_set

    # ==================== 参数解析 ====================

    # ==================== 参数解析 ====================

    def _parse_image_params(self, text: str, strict_size: bool = True) -> Tuple[str, Dict[str, Any]]:
        """解析生图参数：支持数量 + 比例/尺寸。"""
        params = {
            "n": 1,
            "size": None,
            "invalid_size": None,
            "size_explicit": False,
            "stream": None,
        }
        parts = text.split()
        if not parts:
            return "", params

        remaining_text = text.strip()
        if parts[0].isdigit() and 1 <= int(parts[0]) <= self.MAX_IMAGE_COUNT:
            params["n"] = int(parts[0])
            remaining_text = " ".join(parts[1:]).strip()

        remaining_text, stream_preference = self._extract_stream_param_from_text(remaining_text)
        if stream_preference is not None:
            params["stream"] = stream_preference

        remaining_text, size_value, invalid_size = self._extract_size_param_from_text(remaining_text)
        if size_value:
            params["size"] = size_value
            params["size_explicit"] = True
        elif strict_size and invalid_size:
            params["invalid_size"] = invalid_size

        prompt = remaining_text.strip()
        return prompt, params

    def _parse_video_params(self, text: str, strict_size: bool = True) -> Tuple[str, Dict[str, Any]]:
        """解析生视频参数：支持秒数 + 比例/尺寸 + 全局流式覆盖。"""
        params = {
            "video_length": self.DEFAULT_VIDEO_LENGTH_SECONDS,
            "size": None,
            "invalid_size": None,
            "stream": None,
        }
        parts = text.split()
        if not parts:
            return "", params

        remaining_text = text.strip()
        remaining_text, stream_preference = self._extract_stream_param_from_text(remaining_text)
        if stream_preference is not None:
            params["stream"] = stream_preference

        remaining_text, size_value, invalid_size = self._extract_size_param_from_text(remaining_text)
        if size_value:
            params["size"] = size_value
        elif strict_size and invalid_size:
            params["invalid_size"] = invalid_size

        remaining_text, video_length = self._extract_video_length_from_text(remaining_text)
        if video_length is not None:
            params["video_length"] = video_length

        prompt = remaining_text.strip()
        return prompt, params


    # ==================== 命令 ====================

    @staticmethod
    def _chinese_number_to_int(token: str) -> Optional[int]:
        mapping = {
            "一": 1, "二": 2, "两": 2, "三": 3, "四": 4, "五": 5,
            "六": 6, "七": 7, "八": 8, "九": 9, "十": 10
        }
        token = (token or "").strip()
        if not token:
            return None
        if token in mapping:
            return mapping[token]
        # 十一/十二 这类这里不支持，避免误判
        return None

    def _extract_natural_language_image_input(self, text: str, has_reference_image: bool = False) -> Optional[str]:
        """
        将自然语言生图请求提取为现有解析器可识别的格式：
        [数量] 提示词
        例如：
        - 帮我生成两张日落沙滩图片 -> "2 日落沙滩"
        """
        raw = (text or "").strip()
        if not raw:
            return None

        # 命令消息交给命令处理器，避免重复触发
        if raw[:1] in ("#", "/", "!", "！"):
            return None

        low = raw.lower()
        if any(k in low for k in ["生视频", "生成视频", "做视频", "视频"]):
            return None

        # 先拦截“问题反馈/追问”类句子，避免误触发
        negative_markers = [
            "为什么会", "哪里出现了问题", "哪里有问题", "有问题", "不是我要求", "显然不是", "怎么回事", "为啥", "为何", "刚才这段话",
            "没啥问题", "空白区域", "太多了", "自适应", "渲染下方"
        ]
        if any(m in raw for m in negative_markers):
            return None

        # 看图问答/图像理解类请求，不应触发生图
        vision_qa_markers = [
            "这张图片", "图里", "图中", "画面里", "里面有什么", "有什么内容", "看一下这张图", "帮我看下图", "识别这张图"
        ]
        if any(m in raw for m in vision_qa_markers):
            return None

        image_triggers = ["生图", "画图", "画一张", "生成", "画", "制作", "创建"]
        image_nouns = ["图", "图片", "照片", "壁纸", "插画", "画面"]
        has_trigger = any(k in raw for k in image_triggers) and any(n in raw for n in image_nouns)
        has_strong_trigger = any(k in raw for k in ["生图", "画图", "生成图片", "生成一张", "生成两张", "生成三张"])
        edit_triggers = ["改", "修改", "调成", "变成", "换成", "替换", "重绘", "编辑", "润色"]
        has_edit_intent = any(k in raw for k in edit_triggers)

        # 额外要求：句首要像“请求执行”，避免“图片生成不对”这类描述句误触发
        has_request_intent = bool(re.search(r'^(请|麻烦|帮我|给我|来|生成|画|做|制作|创建|弄|整|能不能|可以|请你|把)', raw)) or ("帮我" in raw) or ("给我" in raw)

        natural_img2img = bool(has_reference_image and has_edit_intent and has_request_intent)
        if not (((has_trigger or has_strong_trigger) and has_request_intent) or natural_img2img):
            return None

        # 数量
        count = None
        m_count_digit = re.search(r'(?<!\d)([1-9]|10)\s*张', raw)
        if m_count_digit:
            count = int(m_count_digit.group(1))
        else:
            m_count_cn = re.search(r'(一|二|两|三|四|五|六|七|八|九|十)\s*张', raw)
            if m_count_cn:
                count = self._chinese_number_to_int(m_count_cn.group(1))

        # 提示词粗提取：去掉请求外壳词和参数词
        prompt = raw
        prompt = re.sub(r'^(请|麻烦|帮我|给我|来|请你|可以|能不能|能否|想要|我要|我想要)+', '', prompt)
        prompt = re.sub(r'(帮我|给我|请你)?(生成|生|做|制作|创建|画|画出|画一张|画一下)(一些|几张|一张|两张|三张|四张|五张)?', '', prompt)
        prompt = re.sub(r'(?<!\d)([1-9]|10)\s*张', ' ', prompt)
        prompt = re.sub(r'(一|二|两|三|四|五|六|七|八|九|十)\s*张', ' ', prompt)
        prompt = re.sub(r'(的)?(图片|照片|壁纸|插画|图像|画面|图)(吧|呀|啦|呢|好吗|行吗)?$', '', prompt)
        prompt = re.sub(r'^(来|给|弄|整)\s*', '', prompt)
        prompt = re.sub(r'\s+', ' ', prompt).strip(' ，,。.!！?？')

        if not prompt:
            return None

        parts = []
        if count and 1 <= count <= self.MAX_IMAGE_COUNT:
            parts.append(str(count))
        parts.append(prompt)
        return ' '.join(parts).strip()

    async def _handle_image_request_with_input(self, event: AstrMessageEvent, user_input: str):
        api_key = self.conf.get("grok_api_key", "").strip()
        if not api_key:
            yield event.plain_result("❌ 未配置 API 密钥")
            return

        if not user_input:
            yield event.plain_result("❌ 请输入提示词")
            return

        can_proceed, _ = await self._check_permissions(event)
        if not can_proceed:
            yield event.plain_result("❌ 当前会话无权限使用此功能")
            return

        image_inputs = await self._get_images_from_event(event, max_count=2)
        image_bytes = image_inputs[0] if image_inputs else None
        mask_bytes = image_inputs[1] if len(image_inputs) > 1 else None
        mode = "图生图" if image_bytes else "文生图"

        prompt_text, params = self._parse_image_params(user_input, strict_size=not image_bytes)
        if not prompt_text:
            yield event.plain_result("❌ 请输入提示词")
            return
        if not image_bytes and params.get("invalid_size"):
            yield event.plain_result(
                f"❌ 不支持的比例或尺寸参数: {params['invalid_size']}，支持 1:1 / 3:2 / 2:3 / 16:9 / 9:16"
            )
            return

        if len(prompt_text) > self.MAX_PROMPT_LENGTH:
            yield event.plain_result(f"❌ 提示词过长，最大支持 {self.MAX_PROMPT_LENGTH} 字符")
            return

        image_passthrough_url = self._lookup_generated_image_url(image_bytes) if image_bytes else None

        n = params["n"]
        requested_size = params.get("size")
        target_size = requested_size
        if image_bytes and not target_size:
            source_resolution = self._get_image_resolution(image_bytes)
            if source_resolution:
                target_size = self._get_closest_supported_size(*source_resolution)
        if not target_size:
            target_size = self._get_default_image_size()
        enforce_output_ratio = bool(params.get("size_explicit"))
        stream_preference = params.get("stream")

        yield event.plain_result(f"🎨 正在进行 [{mode}] · {n}张 ...")

        results, error = await self._generate_image(
            prompt_text,
            image_bytes,
            image_url=image_passthrough_url,
            mask_bytes=mask_bytes,
            n=n,
            target_size=target_size,
            stream_preference=stream_preference,
        )

        if error:
            yield event.plain_result(f"❌ [{mode}] 生成失败: {self._translate_error(error)}")
            return

        if not results:
            yield event.plain_result("❌ 未获取到图片")
            return

        images_data = []
        failed_count = 0
        for i, (url_or_path, img_bytes) in enumerate(results):
            media_source = self._normalize_remote_url(url_or_path) or url_or_path
            if img_bytes:
                final_bytes = img_bytes
                if enforce_output_ratio:
                    final_bytes = self._enforce_output_ratio_if_needed(final_bytes, target_size)
                if mode == "文生图":
                    self._remember_generated_image_url(
                        final_bytes,
                        media_source,
                        prefer_generated_url=True,
                    )
                images_data.append((media_source or f"image_{i}", final_bytes))
            elif media_source:
                downloaded = await self._download_media(media_source)
                if downloaded:
                    final_bytes = downloaded
                    if enforce_output_ratio:
                        final_bytes = self._enforce_output_ratio_if_needed(final_bytes, target_size)
                    if mode == "文生图":
                        self._remember_generated_image_url(
                            final_bytes,
                            media_source,
                            prefer_generated_url=True,
                        )
                    images_data.append((media_source, final_bytes))
                else:
                    failed_count += 1

        if not images_data:
            yield event.plain_result("❌ 图片下载失败，请到后台查看")
            return

        if len(images_data) == 1:
            async for result in self._save_and_send_media(event, images_data[0][0], images_data[0][1], "image"):
                yield result
            if failed_count > 0:
                yield event.plain_result(f"⚠️ {failed_count}张图片下载失败，请到后台查看")
        else:
            async for result in self._send_images_forward(event, images_data, failed_count):
                yield result

    def _broadcast_intent_claim(self, event: AstrMessageEvent, intent: str, source: str) -> None:
        """
        向后续插件广播当前事件已被本插件识别并接管。

        - grok_suite.claimed: 是否已接管
        - grok_suite.intent: image / video
        - grok_suite.source: nl / command
        """
        try:
            event.set_extra("grok_suite.claimed", True)
            event.set_extra("grok_suite.intent", intent)
            event.set_extra("grok_suite.source", source)
            # 禁止框架默认 LLM 链路（不影响插件内部自行调用 LLM）
            event.should_call_llm(True)
        except Exception as e:
            logger.debug(f"广播意图接管标记失败: {e}")

    def _extract_natural_language_video_input(self, text: str, has_reference_image: bool = False) -> Optional[str]:
        """
        将自然语言生视频请求提取为现有解析器可识别的格式：
        提示词
        例如：
        - 帮我生成一个日落海边视频 -> "日落海边"
        """
        raw = (text or "").strip()
        if not raw:
            return None

        # 命令消息交给命令处理器
        if raw[:1] in ("#", "/", "!", "！"):
            return None

        low = raw.lower()
        negative_markers = ["为什么会", "哪里出现了问题", "哪里有问题", "有问题", "不是我要求", "显然不是", "怎么回事", "为啥", "为何", "刚才这段话"]
        if any(m in raw for m in negative_markers):
            return None

        has_video_word = any(k in raw for k in ["视频", "短片", "片段", "动图"]) or any(k in low for k in ["video"])
        has_action = any(k in raw for k in ["生成", "生", "做", "制作", "创建", "来", "弄", "整", "让"])
        has_strong_trigger = any(k in raw for k in ["生视频", "生成视频", "做个视频", "制作视频", "grok视频"])
        has_request_intent = bool(re.search(r'^(请|麻烦|帮我|给我|来|生成|做|制作|创建|弄|整|让|能不能|可以|请你)', raw)) or ("帮我" in raw) or ("给我" in raw)

        motion_triggers = [
            "动起来", "动一下", "动一动", "眨眼", "转头", "微笑", "呼吸", "风吹", "摇摆",
            "镜头推进", "镜头拉远", "运镜", "走动", "飘动", "闪烁", "发光", "下雨", "飘雪", "波动"
        ]
        has_motion_intent = any(k in raw for k in motion_triggers)
        natural_img2video = bool(has_reference_image and has_request_intent and has_motion_intent)

        if not ((((has_video_word and has_action) or has_strong_trigger) and has_request_intent) or natural_img2video):
            return None

        prompt = raw
        prompt = re.sub(r'^(请|麻烦|帮我|给我|来|请你|可以|能不能|能否|想要|我要|我想要)+', '', prompt)
        prompt = re.sub(r'(帮我|给我|请你)?(生成|生|做|制作|创建)(一个|一段|一些)?', '', prompt)
        prompt = re.sub(r'(的)?(视频|短片|片段|动图)(吧|呀|啦|呢|好吗|行吗)?$', '', prompt)
        prompt = re.sub(r'^((来|给|弄|整)\s*)+', '', prompt)
        prompt = re.sub(r'\s+', ' ', prompt).strip(' ，,。.!！?？')

        if not prompt:
            return None

        return prompt

    async def _handle_video_request_with_input(self, event: AstrMessageEvent, user_input: str):
        api_key = self.conf.get("grok_api_key", "").strip()
        if not api_key:
            yield event.plain_result("❌ 未配置 API 密钥")
            return

        if not user_input:
            yield event.plain_result("❌ 请输入提示词")
            return

        can_proceed, _ = await self._check_permissions(event)
        if not can_proceed:
            yield event.plain_result("❌ 当前会话无权限使用此功能")
            return

        image_bytes = await self._get_image_from_event(event)
        mode = "图生视频" if image_bytes else "文生视频"

        prompt_text, params = self._parse_video_params(user_input, strict_size=not image_bytes)

        if not prompt_text:
            yield event.plain_result("❌ 请输入提示词")
            return
        if not image_bytes and params.get("invalid_size"):
            yield event.plain_result(
                f"❌ 不支持的比例或尺寸参数: {params['invalid_size']}，支持 1:1 / 3:2 / 2:3 / 16:9 / 9:16"
            )
            return

        if len(prompt_text) > self.MAX_PROMPT_LENGTH:
            yield event.plain_result(f"❌ 提示词过长，最大支持 {self.MAX_PROMPT_LENGTH} 字符")
            return

        image_passthrough_url = self._lookup_generated_image_url(image_bytes) if image_bytes else None

        target_size = params.get("size") or self._get_default_video_size()
        video_length = int(
            params.get("video_length") or self._get_default_video_length_seconds()
        )
        stream_preference = params.get("stream")
        yield event.plain_result(f"🎬 正在进行 [{mode}] · {video_length}秒 ...")

        video_result, error = await self._generate_video(
            prompt_text,
            image_bytes,
            image_url=image_passthrough_url,
            target_size=target_size,
            video_length=video_length,
            stream_preference=stream_preference,
        )

        if error:
            yield event.plain_result(f"❌ [{mode}] 生成失败: {self._translate_error(error)}")
            return

        if not video_result:
            yield event.plain_result("❌ 未获取到视频")
            return

        save_media = self.conf.get("save_media", False)

        if Path(video_result).is_file():
            try:
                if save_media:
                    filename = Path(video_result).name
                    save_path = (self.video_dir / filename).resolve()
                    async with aiofiles.open(video_result, 'rb') as src:
                        content = await src.read()
                    async with aiofiles.open(save_path, 'wb') as dst:
                        await dst.write(content)
                    self._cleanup_directory_to_limit(
                        self.video_dir,
                        self._get_media_file_limit("video", persistent=True),
                    )
                    await aiofiles.os.remove(video_result)
                    component = Comp.Video.fromFileSystem(path=str(save_path), name=filename)
                else:
                    component = Comp.Video.fromFileSystem(path=video_result, name=Path(video_result).name)
                yield event.chain_result([component])
            except Exception as e:
                logger.error(f"视频发送失败: {e}")
                yield event.plain_result(f"❌ 视频发送失败: {self._translate_error(str(e))}")
            finally:
                if not save_media:
                    try:
                        await aiofiles.os.remove(video_result)
                    except Exception:
                        pass
        else:
            video_bytes = await self._download_media(video_result)
            if video_bytes:
                async for result in self._save_and_send_media(event, video_result, video_bytes, "video"):
                    yield result
            else:
                # 回退：下载失败时直接回传 URL，避免用户侧无结果
                yield event.plain_result(f"⚠️ 视频下载失败（可能是上游链接权限限制），可先尝试直链访问：{video_result}")

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def cache_recent_session_images(self, event: AstrMessageEvent):
        """缓存当前会话最近出现的直接图片，供 reply 元数据缺失时兜底。"""
        try:
            direct_images = await self._get_direct_images_from_event(event, max_count=3)
            for image_bytes in direct_images:
                self._remember_recent_session_image(event, image_bytes, source="incoming")
        except Exception as e:
            logger.debug(f"[引用图] 缓存最近会话图片失败: {e}")

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_natural_language_video_request(self, event: AstrMessageEvent):
        """自然语言生视频入口：如“帮我生成一个日落海边视频”"""
        if not self.conf.get("enable_natural_language_video", True):
            return

        raw_text = (getattr(event, "message_str", "") or "").strip()
        if self._looks_like_command_text(raw_text, ["生视频", "grok视频", "grok生视频"]):
            return

        text = self._get_message_text_without_reply(event)
        if not text:
            return

        # 只在被唤醒(@/前缀)或显式提到 grok 时触发，避免误伤普通聊天
        if not event.is_at_or_wake_command and ("grok" not in text.lower()):
            return

        # 先走“无图语义”快速判定，命中后立即 stop_event，避免默认 LLM 抢先回复
        user_input = self._extract_natural_language_video_input(text, has_reference_image=False)
        if user_input:
            self._broadcast_intent_claim(event, intent="video", source="nl")
            event.stop_event()
            async for result in self._handle_video_request_with_input(event, user_input):
                yield result
            return

        # 仅在无图语义未命中时，再检测是否带图触发图生视频
        image_inputs = await self._get_images_from_event(event, max_count=1)
        has_reference_image = bool(image_inputs)
        if not has_reference_image:
            return

        user_input = self._extract_natural_language_video_input(text, has_reference_image=True)
        if not user_input:
            # 图生视频自然语言兜底（严格）：必须是明确请求且包含动效意图
            request_like = bool(re.search(r'^(请|麻烦|帮我|给我|请你|帮忙|劳驾|把)\b', text or '')) or ("帮我" in (text or "")) or ("给我" in (text or ""))
            motion_like = bool(re.search(r'(动起来|动一下|动一动|眨眼|转头|微笑|呼吸|风吹|摇摆|运镜|镜头推进|镜头拉远|走动|飘动|闪烁|发光|下雨|飘雪|波动)', text or ''))
            if request_like and motion_like:
                user_input = text

        if not user_input:
            return

        self._broadcast_intent_claim(event, intent="video", source="nl")
        event.stop_event()
        async for result in self._handle_video_request_with_input(event, user_input):
            yield result

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_natural_language_image_request(self, event: AstrMessageEvent):
        """自然语言生图入口：如“帮我生成两张日落沙滩图片”"""
        if not self.conf.get("enable_natural_language_image", True):
            return

        raw_text = (getattr(event, "message_str", "") or "").strip()
        if self._looks_like_command_text(raw_text, ["生图", "grok生图", "grok画图"]):
            return

        text = self._get_message_text_without_reply(event)
        if not text:
            return

        # 只在被唤醒(@/前缀)或显式提到 grok 时触发，避免误伤普通聊天
        if not event.is_at_or_wake_command and ("grok" not in text.lower()):
            return

        # 先走“无图语义”快速判定，命中后立即 stop_event，避免默认 LLM 抢先回复
        user_input = self._extract_natural_language_image_input(text, has_reference_image=False)
        if user_input:
            self._broadcast_intent_claim(event, intent="image", source="nl")
            event.stop_event()
            async for result in self._handle_image_request_with_input(event, user_input):
                yield result
            return

        # 仅在无图语义未命中时，再检测是否带图触发图生图
        image_inputs = await self._get_images_from_event(event, max_count=1)
        has_reference_image = bool(image_inputs)
        if not has_reference_image:
            return

        user_input = self._extract_natural_language_image_input(text, has_reference_image=True)
        if not user_input:
            # 图生图自然语言兜底（严格）：必须是“明确请求 + 明确对图片做编辑”
            # 避免“图片渲染有问题/能不能改成自适应”这类反馈语句误触发
            request_like = bool(re.search(r'^(请|麻烦|帮我|给我|请你|帮忙|劳驾|把)\b', text or '')) or ("帮我" in (text or "")) or ("给我" in (text or ""))
            edit_like = bool(re.search(r'(改成|修改|调成|变成|换成|替换|重绘|编辑|润色|把.+变成|把.+换成)', text or ''))
            target_image_like = bool(re.search(r'(这张图|这幅图|这图|这张图片|这幅图片|这张照片|图片|照片)', text or ''))
            if request_like and edit_like and target_image_like:
                user_input = text

        if not user_input:
            return

        self._broadcast_intent_claim(event, intent="image", source="nl")
        event.stop_event()
        async for result in self._handle_image_request_with_input(event, user_input):
            yield result

    @filter.command("grok生图", prefix_optional=True)
    async def on_image_request(self, event: AstrMessageEvent):
        """Grok 生图: #生图 [数量] <提示词> [+图片可选]"""
        raw_input = (getattr(event, "message_str", "") or "").strip()
        user_input = self._extract_user_input_from_command(
            raw_input,
            ["生图", "grok生图", "grok画图"],
        )

        if not user_input:
            yield event.plain_result("❌ 请输入提示词\n示例: #生图 一只可爱的猫咪")
            return

        self._broadcast_intent_claim(event, intent="image", source="command")
        async for result in self._handle_image_request_with_input(event, user_input):
            yield result

    @filter.command("grok视频", prefix_optional=True)
    async def on_video_request(self, event: AstrMessageEvent):
        """Grok 生视频: #生视频 <提示词> [+图片可选]"""
        raw_input = (getattr(event, "message_str", "") or "").strip()
        user_input = self._extract_user_input_from_command(
            raw_input,
            ["生视频", "grok视频", "grok生视频"],
        )

        if not user_input:
            yield event.plain_result("❌ 请输入提示词\n示例: #生视频 让画面动起来")
            return

        self._broadcast_intent_claim(event, intent="video", source="command")
        async for result in self._handle_video_request_with_input(event, user_input):
            yield result

    @filter.command("grok", prefix_optional=True)
    async def on_web_search(self, event: AstrMessageEvent):
        """Grok 对话/搜索: /grok <内容> [+图片/语音/文件可选]"""
        raw_input = (getattr(event, "message_str", "") or "").strip()
        normalized_input = raw_input.lstrip("/#!").strip()
        if normalized_input.startswith(("grok生图", "grok视频", "grok帮助")):
            return

        can_proceed, _ = await self._check_permissions(event)
        if not can_proceed:
            yield event.plain_result("❌ 当前会话无权限使用此功能")
            return

        query = self._extract_user_input_from_command(raw_input, ["grok"])
        multimodal_inputs = await self._collect_multimodal_inputs(event)
        has_multimodal = bool(
            multimodal_inputs.get("image_bytes")
            or multimodal_inputs.get("audio_inputs")
            or multimodal_inputs.get("file_inputs")
        )

        if not query and not has_multimodal:
            yield event.plain_result(
                "使用方法: /grok <问题内容> [+图片/语音/文件可选]\n输入 /grok帮助 查看完整说明"
            )
            return

        if query.lower() == "help" or query == "帮助":
            yield event.plain_result(
                "使用方法: /grok <问题内容> [+图片/语音/文件可选]\n输入 /grok帮助 查看完整说明"
            )
            return

        api_key = self.conf.get("grok_api_key", "").strip()
        if not api_key:
            yield event.plain_result("❌ 未配置 API 密钥")
            return

        result = await self._perform_web_search(query, multimodal_inputs)
        yield event.plain_result(self._format_search_result(result))

    @filter.llm_tool(name="grok_web_search")
    async def grok_web_search_tool(self, event: AstrMessageEvent, query: str) -> str:
        """通过 Grok 进行实时联网搜索，获取最新信息和来源"""
        query = (query or "").strip()
        if not query:
            return "搜索失败: 查询不能为空"

        can_proceed, _ = await self._check_permissions(event)
        if not can_proceed:
            return "搜索失败: 当前会话没有权限使用该工具"

        result = await self._perform_web_search(query)
        return self._format_search_result_for_llm(result)

    @filter.on_llm_request()
    async def on_llm_request(self, event: AstrMessageEvent, req: ProviderRequest):
        """启用 Skill 模式时，移除 grok_web_search 工具，避免与外部 skill 重叠。"""
        if not self._search_skill_enabled():
            return

        tool_set = getattr(req, "func_tool", None)
        if FunctionToolManager is not None and isinstance(tool_set, FunctionToolManager):
            req.func_tool = tool_set.get_full_tool_set()
            tool_set = req.func_tool

        if not tool_set:
            return
        if hasattr(tool_set, "remove_tool"):
            tool_set.remove_tool("grok_web_search")
            return
        if isinstance(tool_set, dict):
            tool_set.pop("grok_web_search", None)

    @filter.command("grok帮助", prefix_optional=True)
    async def on_help(self, event: AstrMessageEvent):
        """Grok 帮助: #grok帮助"""
        help_text = (
            "【Grok 生图视频助手】\n\n"
            "🎨 生图命令:\n"
            "#生图 [数量] [比例] 提示词\n"
            "• 数量: 1-10 (默认1)\n"
            "• 比例支持: 1:1 / 3:2 / 2:3 / 16:9 / 9:16\n"
            "• 未指定比例时使用后台配置的默认图片比例\n"
            "• 可附带图片进行图生图\n"
            "• 附带两张图时第2张作为局部重绘蒙版\n\n"
            "示例:\n"
            "• #生图 一只猫\n"
            "• #生图 4 16:9 日落海滩\n"
            "• #生图 把背景换成森林 +图片\n\n"
            "━━━━━━━━━━━━━━\n"
            "🎬 视频命令:\n"
            "#生视频 [秒数] [比例] [流式/非流式] 提示词 [+图片可选]\n"
            "• 可附带图片进行图生视频\n"
            "• 秒数支持: 6-30 秒\n"
            "• 比例支持: 1:1 / 3:2 / 2:3 / 16:9 / 9:16\n"
            "• 未指定秒数/比例时使用后台默认视频秒数与默认视频比例\n"
            "• 分辨率使用后台默认视频分辨率；720p 会触发超分，耗时更久\n"
            "• 流式受全局开关控制，也可在命令里用“流式/非流式”临时覆盖\n\n"
            "示例:\n"
            "• #生视频 让画面动起来\n"
            "• #生视频 10秒 16:9 夜晚海边慢镜头\n"
            "• #生视频 15秒 9:16 流式 让城市霓虹缓慢流动\n"
            "• #生视频 8秒 3:2 让人物眨眼微笑 +图片\n\n"
            "━━━━━━━━━━━━━━\n"
            "💬 对话/联网搜索:\n"
            "/grok 问题内容 [+图片/语音/文件可选]\n"
            "• 支持普通对话、实时联网搜索、图片/语音/文件理解\n"
            "• 通过 grok_search_mode 控制联网策略: auto / on / off\n"
            "• 输入 /grok help 或 /grok帮助 查看说明\n\n"
            "示例:\n"
            "• /grok 今天有什么新闻\n"
            "• /grok 这张图片里有什么 +图片\n"
            "• /grok 帮我总结这个语音和文件 +语音/+文件\n\n"

        )
        yield event.plain_result(help_text)
