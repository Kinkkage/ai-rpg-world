# app/services/llm_models.py
from __future__ import annotations
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field


class LLMMechanics(BaseModel):
    """
    Механическая часть решения LLM:
      - type: "hit" | "miss" | "none"
      - damage: целое число (0..)
      - status: произвольный объект (бафф/дебафф и т.п.)
    """
    type: str = Field(default="none")
    damage: int = Field(default=0)
    status: Optional[Dict[str, Any]] = None


class LLMChoice(BaseModel):
    """
    Кнопки выбора для контратаки/манёвра и т.п.
    """
    label: str
    value: str


class LLMDecision(BaseModel):
    """
    Полный ответ LLM:
      - narration: текст от третьего лица
      - mechanics: механика (урон/промах/ничего)
      - choices: необязательные варианты выбора
    """
    narration: str
    mechanics: LLMMechanics
    choices: Optional[List[LLMChoice]] = None
