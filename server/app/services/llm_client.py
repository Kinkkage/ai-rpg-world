# app/services/llm_client.py
from __future__ import annotations
from typing import Dict, Any, Optional
import os
import json
import logging
import asyncio

from openai import OpenAI  # используем тот же класс, что и в test_openai.py

log = logging.getLogger(__name__)

# --- конфиг из окружения ---
AI_ENABLED = os.getenv("AI_ENABLED", "false").lower() == "true"
AI_MODEL = os.getenv("AI_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
AI_TIMEOUT_MS = int(os.getenv("AI_TIMEOUT_MS", "8000") or "8000")  # чуть увеличим таймаут

log.info(f"[LLM] AI_ENABLED={AI_ENABLED}, MODEL={AI_MODEL}")

_client: Optional[OpenAI] = None


def _get_client() -> Optional[OpenAI]:
    """Лениво создаём клиента OpenAI, используем те же настройки, что и в test_openai.py."""
    global _client
    if not AI_ENABLED:
        log.warning("[LLM] disabled via AI_ENABLED env var")
        return None
    if not OPENAI_API_KEY:
        log.error("[LLM] OPENAI_API_KEY is not set")
        return None
    if _client is None:
        _client = OpenAI(api_key=OPENAI_API_KEY)
    return _client


async def call_llm_json(system_prompt: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Вызывает OpenAI и возвращает РАЗОБРАННЫЙ JSON-объект
    или None при любой ошибке.

    system_prompt — длинный системный промпт (HERO_SYSTEM_PROMPT / NPC_SYSTEM_PROMPT)
    payload — контекст хода (actor/target/inventory/skills/. + say/act)
    """
    client = _get_client()
    if client is None:
        log.warning("[LLM] disabled or client not initialized")
        return None

    # Пробуем понять, кто вызывает — герой или NPC (по тексту промпта)
    # Чуть расширим условие: вдруг промпт поменяется и будет просто "NPC"
    origin = "npc" if ("Отвечаешь ЗА NPC" in system_prompt or "NPC" in system_prompt) else "hero"

    # ВАЖНО: default=str → UUID и прочие нестандартные типы превращаем в строки
    try:
        user_content = json.dumps(payload, ensure_ascii=False, default=str)
    except Exception as e:
        log.exception(f"[LLM:{origin}] error while dumping payload to JSON: {e}")
        return None

    messages = [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_content},
    ]

    timeout_s = AI_TIMEOUT_MS / 1000.0
    log.debug(f"[LLM:{origin}] request start, timeout={timeout_s}s, payload_len={len(user_content)}")

    def _sync_call():
        # Синхронный вызов API, как в test_openai.py, только с response_format
        resp = client.chat.completions.create(
            model=AI_MODEL,
            messages=messages,
            response_format={"type": "json_object"},
        )
        return resp

    try:
        loop = asyncio.get_event_loop()
        resp = await asyncio.wait_for(
            loop.run_in_executor(None, _sync_call),
            timeout=timeout_s,
        )
    except asyncio.TimeoutError:
        log.error(f"[LLM:{origin}] request timeout")
        return None
    except Exception as e:
        log.exception(f"[LLM:{origin}] unexpected error: {e}")
        return None

    # Разбираем ответ
    try:
        content = resp.choices[0].message.content
    except Exception as e:
        log.exception(f"[LLM:{origin}] bad response structure: {e}")
        return None

    if not content:
        log.error(f"[LLM:{origin}] empty content from model")
        return None

    # --- Первая попытка: обычный json.loads ---
    try:
        data = json.loads(content)
    except json.JSONDecodeError as e:
        # КРИТИЧНО ДЛЯ НАС: здесь видно сырое содержимое
        log.error(f"[LLM:{origin}] JSON decode error: {e}; content={content!r}")

        # --- Вторая попытка: "ремонт" JSON ---
        # Иногда модель может вернуть текст с лишними словами, ```json и т.п.
        # Пробуем вырезать кусок от первой '{' до последней '}'.
        start = content.find("{")
        end = content.rfind("}")
        if start != -1 and end != -1 and end > start:
            trimmed = content[start : end + 1]
            try:
                data = json.loads(trimmed)
                log.warning(
                    f"[LLM:{origin}] JSON salvage succeeded after trimming content "
                    f"(len={len(content)} -> {len(trimmed)})"
                )
            except Exception as e2:
                log.error(f"[LLM:{origin}] JSON salvage failed: {e2}; trimmed={trimmed!r}")
                return None
        else:
            # Не нашли в ответе даже фигурные скобки — совсем мусор
            return None
    except Exception as e:
        log.exception(f"[LLM:{origin}] unexpected error while parsing JSON: {e}")
        return None

    if not isinstance(data, dict):
        log.error(f"[LLM:{origin}] response is not a JSON object: {data!r}")
        return None

    log.debug(f"[LLM:{origin}] JSON parsed ok")
    return data


# ---- ВСПОМОГАТЕЛЬНЫЕ ШТУКИ ДЛЯ /debug/llm_ping И /debug/llm_direct ----

def llm_diagnostics() -> Dict[str, Any]:
    """
    Простой пинг-конфиг: что в env и инициализировался ли клиент.
    Используется в /debug/llm_ping.
    """
    client_ok = _get_client() is not None
    return {
        "ai_enabled_env": os.getenv("AI_ENABLED"),
        "model": AI_MODEL,
        "ok": bool(client_ok),
        "raw": None,
    }


async def llm_direct_test(text: str) -> Dict[str, Any]:
    """
    Прямой тест LLM: просто отправляем строку и возвращаем сырой текст ответа.
    Удобно для /debug/llm_direct.
    """
    client = _get_client()
    if client is None:
        return {
            "ok": False,
            "error": "client_not_initialized_or_disabled",
        }

    messages = [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": text},
    ]

    timeout_s = AI_TIMEOUT_MS / 1000.0

    def _sync_call():
        resp = client.chat.completions.create(
            model=AI_MODEL,
            messages=messages,
        )
        return resp

    try:
        loop = asyncio.get_event_loop()
        resp = await asyncio.wait_for(
            loop.run_in_executor(None, _sync_call),
            timeout=timeout_s,
        )
    except asyncio.TimeoutError:
        return {"ok": False, "error": "timeout"}
    except Exception as e:
        log.exception(f"[LLM] direct test error: {e}")
        return {"ok": False, "error": str(e)}

    content = None
    try:
        content = resp.choices[0].message.content
    except Exception as e:
        log.exception(f"[LLM] direct test bad response: {e}")
        return {"ok": False, "error": "bad_response"}

    return {
        "ok": True,
        "model": AI_MODEL,
        "content": content,
    }
