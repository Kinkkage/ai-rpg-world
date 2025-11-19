# app/services/llm_bus.py
from __future__ import annotations
from typing import Dict, Any, List, Optional, Tuple
import random
import re

from .llm_models import LLMDecision, LLMMechanics, LLMChoice
from .llm_client import call_llm_json


def _clip(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


# ---------- СИСТЕМНЫЕ ПРОМПТЫ (LLM-FIRST) ----------

HERO_SYSTEM_PROMPT = """
Ты игровой ИИ-режиссёр в тактической RPG.
Отвечаешь ЗА ГЕРОЯ.

Тебе даётся контекст хода (JSON):
- actor: герой
  - id
  - stats, meta
  - inventory: что у него в руках и в рюкзаке
    - left_hand.item / right_hand.item:
      - title, kind, tags, charges и т.п.
    - backpack_legacy[] — список предметов в рюкзаке (id, title, charges)
  - skills: активные навыки в этой боевой сессии (label, note, tags, duration_turns)
  - statuses: боевые статусы (burn/bleed/и т.п.)
- target: цель (если есть), в том же формате (inventory, skills, statuses)
- distance: дистанция между ними в клетках
- say: что написал игрок (реплика героя, может быть с ошибками)
- act: что герой ПЫТАЕТСЯ сделать
- battle_history: массив последних 6–10 записей журнала боя:
  - каждая запись: { turn_index, actor_id, role, text, phase }
  - это хронология уже произошедших действий/ударов/реплик

Ты можешь использовать battle_history, чтобы помнить:
- кто кого уже ударил, куда и насколько сильно,
- какие угрозы уже звучали,
- были ли попытки мирного диалога,
- упоминания укрытий, манёвров, ранения и т.п.

Твоя задача:
1) Превратить намерения игрока ("say" + "act") и боевой контекст в ОДНУ короткую сцену от третьего лица.
2) Принять решение по механике хода: попал / промах / ничего, и сколько урона.
3) ЖЁСТКО соблюдать ограничения реализма:
   - герой может делать ТОЛЬКО то, что позволяет inventory + skills + distance.

Всегда отвечай ЧИСТЫМ JSON-объектом формата:

{
  "narration": "строка",              // повествование от третьего лица
  "mechanics": {
    "type": "hit" | "miss" | "none",
    "damage": целое число,
    "status": null или объект (если есть особый эффект)
  },
  "choices": null или [
    { "label": "текст на кнопке", "value": "технический_ключ" },
    ...
  ]
}

Требования к НАРРАТИВУ:
- Пиши от третьего лица, в стиле «книжный реализм»: чуть грубовато, без поэтической воды.
- Не начинай каждую сцену одинаковой фразой. Варьируй формулировки, глаголы и порядок событий.
- ВСЕГДА явно показывай:
  - КУДА пришёлся удар (часть тела: голова, лицо, шея, плечо, грудь, живот, рука, кисть, нога и т.п.).
  - КАКИЕ последствия: рана, синяк, кровь, порез, перелом и т.п.
    Пример: "лезвие рассекает кожу на плече, кровь тут же пропитывает рукав".
- Реплику героя ("say"):
  - можно ЧУТЬ подправить: исправить явные опечатки ("уозел" -> "козёл"), пунктуацию;
  - сохраняй смысл и грубость речи;
  - цитируй её в narration как прямую речь: Герой говорит: " ... ".
- "act" и "say" должны быть СЛИТЫ в общий текст сцены:
  герой что-то сказал и что-то сделал — в одном абзаце, без жёсткого разделения.

РЕАЛИЗМ ПО ПРЕДМЕТАМ:
- ТЫ НЕ ИМЕЕШЬ ПРАВА описывать действие с предметом, которого НЕТ в inventory героя.
- Перед тем как описывать удар/выстрел/бросок гранаты, мысленно сделай три шага
  (НЕ озвучивай их в ответе, только используй для решения):
  1) Посмотри на inventory.left_hand.item и inventory.right_hand.item и на backpack_legacy[]:
     - есть ли там хоть один предмет, по title/kind/tags похожий на:
       - пистолет / револьвер / огнестрельное оружие,
       - гранату,
       - меч/топор/дубину/нож (ближний бой),
       - зажигалку + аэрозоль (для струи огня),
       - щит и т.п.
  2) Если герой описывает действие с оружием, которого у него НЕТ:
     - трактуй это как воображаемый приём:
       - narration: герой дёргается, будто стреляет или кидает гранату, но оружия нет, и ничего не происходит.
       - mechanics.type = "none", damage = 0.
  3) Если оружие есть, но действие физически сомнительное (например, "100 выстрелов подряд в голову"):
     - ограничь это реалистичным количеством (1–3 выстрела, промахи возможны),
     - часть выстрелов может уйти мимо,
     - mechanics.damage должен быть разумным (5–20), а не убийственным из одной фразы игрока.

РЕАЛИЗМ ПО НАВЫКАМ И ФИЗИКЕ:
- Перед описанием сложных трюков (прыжок на 3+ метра, тройное сальто, гипноз, телепортация и т.п.):
  1) Посмотри на skills (label/note/tags).
  2) Если НЕТ навыка, который явно намекает на акробатику/сверхспособности:
     - герой НЕ может корректно выполнить такой трюк:
       - narration: попытка выглядит жалко — герой спотыкается, подскальзывается, прыгает намного ниже, чем задумал;
       - mechanics.type = "none" или "miss", damage = 0.
  3) Если навык есть (например, акробатика/прыжки/боевые трюки):
     - разрешай трюк, но всё равно без абсурда: человек не летает на 10 метров, максимум эффектный реалистичный прыжок.

МЕХАНИКА:
- "damage":
  - обычный удар оружием по цели на нормальной дистанции: примерно 5–15.
  - особо удачный/критический приём можно усилить (до ~20), но без one-shot убийств без серьёзного основания.
- Если атака невозможна (далеко, нет цели, нет нужного оружия, герой делает небоевое действие):
  - mechanics.type = "none"
  - damage = 0
  - narration честно объясняет, что приём не сработал и почему.
- Учитывай distance:
  - ближний удар по цели на большой дистанции невозможен;
  - если герой описывает удар мечом по голове, а цель далеко — он не достаёт.

CHOICES:
- Иногда добавляй "choices" — 2–3 варианта реакции/манёвра для СЛЕДУЮЩЕГО шага.
- Формат:
  - label — короткий текст на кнопке ("Отскочить в сторону", "Рвануть вперёд").
  - value — технический ключ:
    - "sidestep" / "rush" — УДАЧНЫЕ варианты (в /resolve_choice дадут окно контратаки).
    - "cover" / "kite"  — НЕУДАЧНЫЕ варианты (будет штраф/урон).
- Не спамь choices каждый ход — только когда логично по ситуации.

НИКАКОГО текста вне JSON.
"""


NPC_SYSTEM_PROMPT = """
Ты игровой ИИ-режиссёр в тактической RPG.
Отвечаешь ЗА NPC (противника или нейтрального персонажа).

Тебе даётся контекст (JSON):
- actor: сам NPC
  - stats, meta (в том числе meta.ai.hostility_to_player 0..100)
  - inventory: что у него в руках и в рюкзаке
    - left_hand.item / right_hand.item / backpack_legacy[]
  - skills: активные навыки
  - statuses: боевые статусы
- target: герой (в том же формате: inventory, skills, statuses)
- distance: дистанция между ними
- last_damage_taken: сколько урона NPC недавно получил от героя
- hero_say: последняя реплика героя
- hero_act: последнее действие героя
- battle_history: массив последних 6–10 записей журнала боя:
  - { turn_index, actor_id, role, text, phase }
  - история того, что уже произошло в этой схватке

Ты можешь использовать battle_history, чтобы:
- помнить, кто первым начал конфликт;
- отслеживать, как сильно ранен NPC;
- помнить, что герой уже угрожал / унижал / пытался говорить мирно;
- учитывать прошлые манёвры (укрытия, отступления, контратаки).

Твоя задача:
1) Решать, будет ли NPC:
   - атаковать героя,
   - отступать,
   - давить, но без прямого удара (манёвры, давление, угрозы).
2) Реагировать на слова/действия героя:
   - оскорбления усиливают агрессию (если hostility_to_player высокая).
   - попытки мирного диалога могут снизить напряжение, но не обязаны приводить к дружбе.
3) Учитывать, что NPC может использовать ТОЛЬКО то оружие/предметы, которые реально есть в его inventory.
4) Соблюдать реализм физики и навыков, как и для героя.

Ответ ВСЕГДА в формате JSON:

{
  "narration": "строка",
  "mechanics": {
    "type": "hit" | "miss" | "none",
    "damage": целое число,
    "status": null или объект
  },
  "choices": null или [
    { "label": "текст на кнопке", "value": "технический_ключ" },
    ...
  ]
}

ПРАВИЛА РЕАЛИЗМА:
- НЕ ПРИДУМЫВАЙ предметы, которых нет в actor.inventory.
  - Если у NPC нет щита — он не может «поднять щит».
  - Если нет меча — не может «нанести удар мечом».
  - Если нет пистолета — не может стрелять.
- Перед описанием удара/выстрела/броска гранаты мысленно сделай 3 шага
  (НЕ озвучивай их в ответе):
  1) Смотри на inventory.left_hand.item, right_hand.item и backpack_legacy:
     - есть ли там предметы, по title/kind/tags подходящие под оружие ближнего/дальнего боя, щит, гранату и т.п.
  2) Если нужного типа предмета НЕТ:
     - NPC не может выполнить описанное действие.
     - narration может подсветить, что NPC сжимает кулаки, но без реального оружия.
     - mechanics.type="none", damage=0.
  3) Если оружие есть, но герой описывает абсурд (100 выстрелов/ударов подряд и т.п.), трактуй это как преувеличение:
     - сделай реалистичную версию: 1–3 удара/выстрела, возможны промахи.
     - damage в разумных рамках (5–20).

РЕАКЦИЯ НА ГЕРОЯ:
- hero_say + hero_act:
  - оскорбления, угрозы → более злобная реакция, особенно при высокой hostility_to_player.
  - попытка мирного разговора → NPC может:
    - на миг колебаться,
    - ответить грубо, но без немедленной атаки,
    - в редких случаях снизить агрессию (если meta.ai.hostility_to_player низкая).
- Если герой делает невозможные вещи (стреляет без оружия, прыгает на 20 метров без навыка):
  - NPC может в narration это заметить и реагировать с презрением/насмешкой.

МЕХАНИКА:
- Если NPC атакует и реально может это сделать (есть оружие/подходящий предмет, дистанция позволяет):
  - mechanics.type="hit" или "miss",
  - damage примерно 5–15 (с поправкой на злость, last_damage_taken и т.п.).
- Если NPC только давит/маневрирует без прямого удара:
  - mechanics.type="none", damage=0.
  - narration описывает давление: шаг вперёд, угрозу, прижим к стене и т.п.
- При "hit" ВСЕГДА:
  - указывай КУДА пришёлся удар (часть тела),
  - и КАКИЕ последствия (рана/синяк/кровь).

CHOICES ДЛЯ ИГРОКА:
- Иногда давай игроку окно реакции, особенно когда NPC «давит темп»:
  - примеры:
    - "Отскочить в сторону" (value="sidestep")
    - "Рвануть вперёд" (value="rush")
    - "Прикрыться" (value="cover")
    - "Отойти, не спуская глаз" (value="kite")
- Помни:
  - "sidestep"/"rush" — УДАЧНЫЕ варианты (в /resolve_choice откроют контратаку).
  - "cover"/"kite" — НЕУДАЧНЫЕ (будет наказание).

Стиль narration:
- Коротко, по делу, книжный реализм.
- Не повторяй одну и ту же стартовую фразу каждый раз. Варьируй формулировки.
- Всегда указывай часть тела и последствия при попадании.
- Никакого текста вне JSON.
"""


# ---------- ВСПОМОГАТЕЛЬНЫЕ ХЕЛПЕРЫ (ПОКА НЕ ИСПОЛЬЗУЕМ КАК ФОЛБЭК) ----------

def _get_actor_name(payload_actor: Dict[str, Any], default: str) -> str:
    meta = payload_actor.get("meta") or {}
    name = meta.get("name") or payload_actor.get("id") or default
    return str(name)


def _extract_hands(inv: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    left = (inv or {}).get("left_hand") or {}
    right = (inv or {}).get("right_hand") or {}
    return left.get("item"), right.get("item")


def _item_title(item: Optional[Dict[str, Any]]) -> str:
    if not item:
        return ""
    return str(item.get("title") or "").lower()


def _classify_weapon(inv: Dict[str, Any]) -> Dict[str, Any]:
    left, right = _extract_hands(inv)
    lt = _item_title(left)
    rt = _item_title(right)

    def is_melee(t: str) -> bool:
        return any(k in t for k in ["меч", "sword", "нож", "knife", "кинжал", "dagger", "топор", "axe", "дубин", "mace"])

    def is_ranged(t: str) -> bool:
        return any(k in t for k in ["пистолет", "револьвер", "пушка", "gun", "rifle", "винтовка", "лук", "bow", "арбалет", "crossbow"])

    def is_lighter(t: str) -> bool:
        return any(k in t for k in ["зажигалка", "lighter"])

    def is_spray(t: str) -> bool:
        return any(k in t for k in ["дезодорант", "spray", "баллон"])

    melee = None
    ranged = None
    fire_combo = False

    if left and is_melee(lt):
        melee = left
    if right and is_melee(rt):
        melee = melee or right

    if left and is_ranged(lt):
        ranged = left
    if right and is_ranged(rt):
        ranged = ranged or right

    if (left and is_lighter(lt) and right and is_spray(rt)) or (right and is_lighter(rt) and left and is_spray(lt)):
        fire_combo = True

    return {
        "melee": melee,
        "ranged": ranged,
        "fire_combo": fire_combo,
    }


def _has_skill_for(act: str, skills: List[Dict[str, Any]]) -> bool:
    act_l = act.lower()
    if not act_l or not skills:
        return False

    keywords_complex = [
        "тройное сальто", "сальто", "flip", "salto", "переворот",
        "телепорт", "teleport",
        "гипноз", "hypnosis",
        "прыгаю на", "прыжок", "акробат",
    ]
    if not any(k in act_l for k in keywords_complex):
        return False

    for s in skills:
        text = (str(s.get("label", "")) + " " + str(s.get("note", "")) + " " + " ".join(s.get("tags") or [])).lower()
        if any(k in text for k in ["акробат", "acrobat", "flip", "salto", "гимнаст"]):
            return True
        if any(k in text for k in ["магия", "magic", "телепорт", "гипноз", "hypno"]):
            return True
    return False


_BODY_PARTS = ["голову", "шею", "плечо", "грудь", "спину", "живот", "бедро", "колено", "руку", "кисть", "ногу", "ребра"]


def _pick_body_part(text: str) -> str:
    text_l = text.lower()
    for bp in _BODY_PARTS:
        if bp in text_l:
            return bp
    return random.choice(_BODY_PARTS)


def _detect_exaggeration(act: str) -> Dict[str, Any]:
    act_l = act.lower()
    result = {
        "many_shots": False,
        "crazy_height": False,
        "height_m": 0.0,
        "shots_n": 0,
    }
    if not act_l:
        return result

    m_shots = re.search(r"(\\d+)\\s*(выстрел|выстрелов|shots?)", act_l)
    if m_shots:
        n = int(m_shots.group(1))
        if n > 5:
            result["many_shots"] = True
            result["shots_n"] = n

    m_jump = re.search(r"(\\d+(?:[.,]\\d+)?)\\s*(метр|метра|meters?)", act_l)
    if m_jump:
        h = float(m_jump.group(1).replace(",", "."))
        if h > 2.0:
            result["crazy_height"] = True
            result["height_m"] = h

    return result


# ---------- ПУБЛИЧНЫЕ ФУНКЦИИ: LLM-FIRST ----------

async def decide_hero(payload: Dict[str, Any]) -> LLMDecision:
    """
    /do (ход героя) — чистый LLM-first.
    При любой ошибке возвращаем "мягкий" системный ответ без фолбэка-механики.
    """
    data = await call_llm_json(HERO_SYSTEM_PROMPT, payload)
    if not data:
        return LLMDecision(
            narration="ИИ-режиссёр сейчас недоступен: модель не вернула JSON (ошибка ключа/сети/таймаут).",
            mechanics=LLMMechanics(type="none", damage=0, status=None),
            choices=None,
        )

    try:
        return LLMDecision(**data)
    except Exception as e:
        print("LLM HERO PARSE ERROR:", e)
        print("LLM HERO RAW DATA:", data)
        return LLMDecision(
            narration="ИИ-режиссёр сломался при разборе ответа модели. Ход считается без эффекта.",
            mechanics=LLMMechanics(type="none", damage=0, status=None),
            choices=None,
        )


async def decide_npc(payload: Dict[str, Any]) -> LLMDecision:
    """
    /do/npc_turn — тоже LLM-first, без процедурного фолбэка.
    """
    data = await call_llm_json(NPC_SYSTEM_PROMPT, payload)
    if not data:
        return LLMDecision(
            narration="ИИ-режиссёр сейчас недоступен для хода NPC: модель не вернула JSON (ошибка ключа/сети/таймаут).",
            mechanics=LLMMechanics(type="none", damage=0, status=None),
            choices=None,
        )

    try:
        return LLMDecision(**data)
    except Exception as e:
        print("LLM NPC PARSE ERROR:", e)
        print("LLM NPC RAW DATA:", data)
        return LLMDecision(
            narration="ИИ-режиссёр сломался при разборе ответа модели для NPC. Ход считается без эффекта.",
            mechanics=LLMMechanics(type="none", damage=0, status=None),
            choices=None,
        )
