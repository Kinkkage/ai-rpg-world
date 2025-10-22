# tests/test_battle_start.py
import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_battle_start_and_state(client: AsyncClient):
    # 1) Засеем игрока в дефолтную сцену (идемпотентно)
    r = await client.post("/debug/seed_state", json={
        "node_id": "forest_path_9596da",
        "x": 5, "y": 5,
        "actor_id": "player",
    })
    assert r.status_code == 200, r.text

    # 2) Узнаем node игрока
    r = await client.get("/debug/state")
    assert r.status_code == 200, r.text
    nid = r.json()["actor"]["node_id"]

    # 3) Создадим противника на той же клетке
    #    (debug/seed_state создаёт актёра kind='player', но для боя это не критично)
    r = await client.post("/debug/seed_state", json={
        "node_id": nid,
        "x": 5, "y": 5,
        "actor_id": "enemy_1",
    })
    assert r.status_code == 200, r.text

    # 4) Старт боя
    res = await client.post("/battle/start", json={
        "node_id": nid,
        "actor_ids": ["player", "enemy_1"]
    })
    assert res.status_code == 200, res.text
    js = res.json()
    assert js.get("ok") is True
    sid = js["session_id"]

    # 5) Чтение состояния боя
    state = await client.get(f"/battle/state/{sid}")
    assert state.status_code == 200, state.text
    data = state.json()
    assert data.get("ok") is True
    parts = data.get("participants", [])
    assert isinstance(parts, list) and len(parts) >= 2
    # опциональные базовые инварианты
    assert data.get("session", {}).get("node_id") == nid
    assert data.get("session", {}).get("state") in ("running", "finished")
