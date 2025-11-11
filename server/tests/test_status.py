#server/tests/test_status.py
import pytest
from httpx import AsyncClient

@pytest.mark.asyncio
async def test_apply_and_list_status(client: AsyncClient, ensure_player):
    # навесим guard на 2 хода
    r = await client.post("/world/status/apply", json={
        "actor_id": "player",
        "status_id": "guard",
        "turns_left": 2,
        "intensity": 1.0
    })
    assert r.status_code == 200
    js = r.json()
    assert js.get("ok") is True
    assert js.get("applied") == "guard"

    # проверим, что появился
    r2 = await client.get("/world/status/player")
    assert r2.status_code == 200
    arr = r2.json()
    assert any(s.get("status_id") == "guard" for s in arr)


@pytest.mark.asyncio
async def test_advance_turn_tick_and_expire(client: AsyncClient, ensure_player):
    # guard на 1 ход
    r0 = await client.post("/world/status/apply", json={
        "actor_id": "player",
        "status_id": "guard",
        "turns_left": 1
    })
    assert r0.status_code == 200

    # продвигаем ход
    r1 = await client.post("/world/turn/advance")
    assert r1.status_code == 200
    js1 = r1.json()
    assert js1.get("ok") is True

    # guard должен исчезнуть
    r2 = await client.get("/world/status/player")
    assert r2.status_code == 200
    arr = r2.json()
    assert all(s.get("status_id") != "guard" for s in arr)


@pytest.mark.asyncio
async def test_remove_status(client: AsyncClient, ensure_player):
    # вешаем rage, потом снимаем
    r0 = await client.post("/world/status/apply", json={
        "actor_id": "player",
        "status_id": "rage",
        "turns_left": 3
    })
    assert r0.status_code == 200

    r1 = await client.post("/world/status/remove", json={
        "actor_id": "player",
        "status_id": "rage"
    })
    assert r1.status_code == 200
    js = r1.json()
    assert js.get("ok") is True

    r2 = await client.get("/world/status/player")
    assert r2.status_code == 200
    arr = r2.json()
    assert all(s.get("status_id") != "rage" for s in arr)
