-- server/db/schema.sql
-- Включаем генератор UUID (pgcrypto)
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1) Узлы/локации
CREATE TABLE IF NOT EXISTS nodes (
  id         TEXT PRIMARY KEY,
  title      TEXT NOT NULL,
  biome      TEXT DEFAULT 'castle',
  size_w     INT  NOT NULL,
  size_h     INT  NOT NULL,
  layout     JSONB DEFAULT '{}'::jsonb,
  exits      JSONB DEFAULT '[]'::jsonb,
  created_at timestamptz DEFAULT now()
);

-- 2) Акторы
CREATE TABLE IF NOT EXISTS actors (
  id         TEXT PRIMARY KEY,
  node_id    TEXT REFERENCES nodes(id) ON DELETE SET NULL,
  kind       TEXT NOT NULL,
  archtype   TEXT DEFAULT NULL,
  x          INT NOT NULL DEFAULT 0,
  y          INT NOT NULL DEFAULT 0,
  hp         INT NOT NULL DEFAULT 100,
  mood       TEXT DEFAULT 'neutral',
  trust      INT NOT NULL DEFAULT 50,
  meta       JSONB DEFAULT '{}'::jsonb
);

-- 3) Память NPC
CREATE TABLE IF NOT EXISTS npc_memories (
  id         bigserial PRIMARY KEY,
  actor_id   TEXT REFERENCES actors(id) ON DELETE CASCADE,
  ts         timestamptz DEFAULT now(),
  event      TEXT NOT NULL,
  payload    JSONB DEFAULT '{}'::jsonb
);

-- 4) Справочник типов предметов
CREATE TABLE IF NOT EXISTS item_kinds (
  id             TEXT PRIMARY KEY,
  title          TEXT NOT NULL,
  description    TEXT DEFAULT '',
  tags           TEXT[] DEFAULT '{}',
  handedness     TEXT DEFAULT 'one_hand',
  stackable      BOOLEAN DEFAULT FALSE,
  base_charges   INT  DEFAULT 0,
  base_durability INT DEFAULT 0,
  props          JSONB DEFAULT '{}'::jsonb
);

-- 5) Экземпляры предметов
CREATE TABLE IF NOT EXISTS items (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  kind_id      TEXT REFERENCES item_kinds(id) ON DELETE RESTRICT,
  owner_actor  TEXT REFERENCES actors(id) ON DELETE SET NULL,
  node_id      TEXT REFERENCES nodes(id) ON DELETE SET NULL,
  charges      INT,
  durability   INT,
  meta         JSONB DEFAULT '{}'::jsonb,
  created_at   timestamptz DEFAULT now()
);

-- 6) Инвентарь (две руки + рюкзак)
CREATE TABLE IF NOT EXISTS inventories (
  actor_id   TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
  left_item  UUID REFERENCES items(id) ON DELETE SET NULL,
  right_item UUID REFERENCES items(id) ON DELETE SET NULL,
  backpack   UUID[] DEFAULT '{}'
);

-- 7) Факты/состояния узла
CREATE TABLE IF NOT EXISTS facts (
  id       bigserial PRIMARY KEY,
  node_id  TEXT REFERENCES nodes(id) ON DELETE CASCADE,
  k        TEXT NOT NULL,
  v        JSONB DEFAULT '{}'::jsonb,
  UNIQUE (node_id, k)
);

-- === ДЕМО ДАННЫЕ ===

INSERT INTO nodes (id, title, biome, size_w, size_h, exits)
VALUES (
  'castle_hall','Зал замка','castle',16,16,
  '[{"id":"to_courtyard","x":0,"y":8,"to":"castle_courtyard"}]'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO actors (id, node_id, kind, archtype, x, y)
VALUES 
('player','castle_hall','player',NULL,5,5),
('king','castle_hall','npc','king',10,6)
ON CONFLICT (id) DO NOTHING;

INSERT INTO inventories (actor_id) VALUES ('player')
ON CONFLICT (actor_id) DO NOTHING;

INSERT INTO item_kinds (id, title, description, tags, handedness, base_charges, props)
VALUES
('lighter','Зажигалка','Карманная зажигалка', ARRAY['tool','fire'],'one_hand',50,'{"ignite":true,"consumes_per_use":1}'),
('deodorant','Дезодорант','Аэрозоль, легко воспламеним', ARRAY['spray','flammable'],'one_hand',20,'{"spray":true,"flammable":true,"consumes_per_use":1}'),
('water_bottle','Бутылка воды','Питьё и тушение огня', ARRAY['liquid','water'],'one_hand',3,'{"water":true,"consumes_per_use":1}')
ON CONFLICT (id) DO NOTHING;

WITH ins AS (
  INSERT INTO items (kind_id, owner_actor, charges)
  VALUES 
    ('lighter','player',50),
    ('deodorant','player',20),
    ('water_bottle','player',3)
  RETURNING id
)
UPDATE inventories
SET backpack = COALESCE(backpack,'{}') || ARRAY(SELECT id FROM ins)
WHERE actor_id = 'player';

-- === Глава 4: Расходники и двуручность =========================

-- Убедимся, что у item_kinds есть все нужные поля
ALTER TABLE item_kinds
  ADD COLUMN IF NOT EXISTS handedness TEXT DEFAULT 'one_hand',
  ADD COLUMN IF NOT EXISTS base_durability INT DEFAULT 100,
  ADD COLUMN IF NOT EXISTS props JSONB DEFAULT '{}'::jsonb;

-- Пример двуручного оружия (greatsword)
INSERT INTO item_kinds (id, title, description, tags, handedness, base_durability, props)
VALUES (
  'greatsword',
  'Тяжёлый меч',
  'Двуручное оружие, требует обе руки. Мощный, но тяжёлый.',
  ARRAY['melee','sword'],
  'two_hands',
  100,
  '{"projectile":false,"consumes_per_use":1}'
)
ON CONFLICT (id) DO NOTHING;

-- Создадим экземпляр меча у игрока
WITH new_item AS (
  INSERT INTO items (kind_id, owner_actor, charges, durability)
  VALUES ('greatsword','player', NULL, 100)
  RETURNING id
)
UPDATE inventories
SET backpack = COALESCE(backpack,'{}') || ARRAY(SELECT id FROM new_item)
WHERE actor_id = 'player';

-- === Дополнительно: проверить навыки (на случай, если не было добавлено ранее)
CREATE TABLE IF NOT EXISTS skills (
  id            TEXT PRIMARY KEY,
  title         TEXT NOT NULL,
  description   TEXT DEFAULT '',
  tags          TEXT[] DEFAULT '{}',
  min_level     INT DEFAULT 1,
  props         JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS actor_skills (
  actor_id      TEXT REFERENCES actors(id) ON DELETE CASCADE,
  skill_id      TEXT REFERENCES skills(id) ON DELETE CASCADE,
  learned_at    timestamptz DEFAULT now(),
  PRIMARY KEY (actor_id, skill_id)
);

ALTER TABLE actors
  ADD COLUMN IF NOT EXISTS level INT DEFAULT 1,
  ADD COLUMN IF NOT EXISTS skill_tokens INT DEFAULT 0;

-- Примеры навыков
INSERT INTO skills (id, title, description, tags, min_level, props) VALUES
('acro_triple_flip','Тройное сальто','Сложный акробатический трюк',['acrobatics'],12,'{}'),
('sword_heavy_slash','Мощный рубящий удар','Сильная атака мечом',['melee','sword'],3,'{}')
ON CONFLICT (id) DO NOTHING;
