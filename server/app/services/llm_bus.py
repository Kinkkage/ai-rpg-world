# app/services/llm_bus.py
from __future__ import annotations
from typing import Dict, Any, List, Optional
import random


def _clip(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


def narrate_third_person(subject_name: str, say: str, act: str, outcome: str) -> str:
    """
    Простой «книжный» тон от третьего лица без LLM.
    """
    say = (say or "").strip()
    act = (act or "").strip()

    say_part = f"— {say} — произнёс {subject_name}." if say else ""
    act_part = f"{subject_name.capitalize()} {act}." if act else ""
    out_part = f" Итог: {outcome}." if outcome else ""

    s = " ".join(x for x in [say_part, act_part, out_part] if x)
    return s or f"{subject_name.capitalize()} огляделся."


def llm_decide_hero(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Заглушка героевого решения.
    Возвращает:
      {
        "narration": str,
        "mechanics": {"type": "hit|miss|none", "damage": int, "status": Optional[dict]},
        "choices": Optional[List[{"label":..., "value":...}]]
      }
    """
    hero = payload["actor"]["id"]
    say  = payload.get("say", "")
    act  = payload.get("act", "")

    target = payload.get("target")
    dist   = int(payload.get("distance", 0) or 0)

    text_act = (act or "").lower()
    wants_melee  = ("удар" in text_act) or ("strike" in text_act)
    wants_ranged = ("выстрел" in text_act) or ("shoot" in text_act)

    outcome = "не произошло ничего особенного"
    mech: Dict[str, Any] = {"type": "none", "damage": 0, "status": None}

    # Ближний бой
    if target and dist <= 1 and wants_melee:
        if random.random() < 0.2:  # 20% промах
            mech["type"] = "miss"
            outcome = "промахнулся"
        else:
            base = int(payload.get("actor", {}).get("stats", {}).get("damage", 8))
            dmg = int(round(base * random.uniform(0.75, 1.25)))
            mech["type"] = "hit"
            mech["damage"] = _clip(dmg, 1, 9999)
            outcome = f"нанёс удар ({mech['damage']} урона)"

    # Дальний бой
    elif target and dist > 1 and wants_ranged:
        if random.random() < 0.3:  # 30% промах
            mech["type"] = "miss"
            outcome = "стрела ушла мимо"
        else:
            base = int(payload.get("actor", {}).get("stats", {}).get("damage", 8))
            dmg = int(round(base * random.uniform(0.6, 1.1)))
            mech["type"] = "hit"
            mech["damage"] = _clip(dmg, 1, 9999)
            outcome = f"попал стрелой ({mech['damage']} урона)"

    else:
        # Небоевой или подготовительный манёвр: для UI можно иногда вернуть «кнопки»
        return {
            "narration": narrate_third_person(hero, say, act, "готовится к манёвру"),
            "mechanics": mech,
            "choices": [
                {"label": "Шаг вбок", "value": "sidestep"},
                {"label": "Прикрыть голову и отступить", "value": "cover_and_back"},
            ],
        }

    return {
        "narration": narrate_third_person(hero, say, act, outcome),
        "mechanics": mech,
        "choices": None,
    }


def llm_decide_npc(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Заглушка НПС. Учитывает:
      - actor.meta.hostility (0..100)
      - last_damage_taken (реактивность)
      - distance (в ближнем чаще даём контру)
    Иногда отдаёт две кнопки контратаки:
      - «Сделать шаг вбок» (хороший исход)
      - «Закрыться руками» (плохой исход)
    """
    npc   = payload["actor"]["id"]
    host  = int(payload.get("actor", {}).get("meta", {}).get("hostility", 50))
    took  = int(payload.get("last_damage_taken", 0))
    dist  = int(payload.get("distance", 1))

    mech: Dict[str, Any] = {"type": "none", "damage": 0, "status": None}
    outcome = ""

    # Чем злее и чем больше урона получил — тем агрессивнее.
    attack_bias = host / 100.0 + min(took / 30.0, 1.0)  # 0..~2

    # Триггер «контры» (кнопок): ближний бой или ощутимый полученный урон
    want_counter = (dist <= 1 or took >= 10) and (random.random() < 0.7)

    if want_counter:
        return {
            "narration": f"{npc.capitalize()} давит и навязывает темп — у вас есть доля секунды на реакцию.",
            "mechanics": mech,
            "choices": [
                {"label": "Сделать шаг вбок", "value": "sidestep"},  # удачный
                {"label": "Закрыться руками", "value": "cover"},     # неудачный
            ],
        }

    # Обычный атакующий ход
    if attack_bias > 0.6:
        if random.random() < 0.2:
            mech["type"] = "miss"
            outcome = "атаковал, но промахнулся"
        else:
            base = int(payload.get("actor", {}).get("stats", {}).get("damage", 6))
            scale = 1.0 + (host / 200.0) + (min(took, 30) / 100.0)  # ярость слегка усиливает
            dmg = int(round(base * random.uniform(0.8, 1.2) * scale))
            mech["type"] = "hit"
            mech["damage"] = _clip(dmg, 1, 9999)
            outcome = f"атаковал ({mech['damage']} урона)"
    else:
        outcome = "пятится и ищет позицию"

    return {
        "narration": f"{npc.capitalize()} {outcome}.",
        "mechanics": mech,
        "choices": None,
    }
