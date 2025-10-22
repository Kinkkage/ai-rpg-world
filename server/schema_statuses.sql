-- Status System 2.0 — таблица активных эффектов на актёрах
create table if not exists actor_statuses (
  id          bigserial primary key,
  actor_id    text not null references actors(id) on delete cascade,
  status_id   text not null,              -- 'burn', 'bleed', 'stun', 'guard', 'rage', ...
  turns_left  int not null default 1,     -- сколько ходов осталось
  stacks      int not null default 1,     -- уровень (если накапливается)
  intensity   double precision not null default 1.0, -- сила эффекта
  source_id   text,                       -- кто наложил
  created_at  timestamptz default now(),
  unique (actor_id, status_id)
);

create index if not exists idx_actor_statuses_actor on actor_statuses(actor_id);
create index if not exists idx_actor_statuses_status on actor_statuses(status_id);

-- Нарративный стиль для описаний статусов (используем позже)
insert into narrative_styles(id,title,config) values
('status','Статус-эффекты','{"tone":"urgent","max_chars":180,"persona":"battle_observer"}')
on conflict (id) do nothing;
