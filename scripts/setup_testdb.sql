-- sql-autoresearch synthetic test database
-- Models a B2B SaaS platform: teams buy products, users generate events.
-- Designed to produce realistic slow-query patterns at scale.
--
-- Usage:
--   createdb autoresearch_test
--   psql autoresearch_test < scripts/setup_testdb.sql
--
-- Takes ~2-4 minutes on an M-series Mac. ~2-3GB on disk.

SET client_min_messages = warning;

BEGIN;

-- ════════════════════════════════════════════════════════════════════
--  Schema
-- ════════════════════════════════════════════════════════════════════

DROP TABLE IF EXISTS events CASCADE;
DROP TABLE IF EXISTS line_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS teams CASCADE;

CREATE TABLE teams (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    plan        text NOT NULL,            -- 'free', 'starter', 'pro', 'enterprise'
    created_at  timestamptz NOT NULL,
    country     text NOT NULL
);

CREATE TABLE users (
    id          serial PRIMARY KEY,
    team_id     int NOT NULL REFERENCES teams(id),
    email       text NOT NULL,
    role        text NOT NULL,            -- 'admin', 'member', 'viewer'
    is_active   boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL,
    last_login  timestamptz
);

CREATE TABLE products (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    category    text NOT NULL,            -- 'compute', 'storage', 'network', 'support', 'addon'
    unit_price  numeric(10,2) NOT NULL,
    is_active   boolean NOT NULL DEFAULT true,
    created_at  timestamptz NOT NULL
);

CREATE TABLE orders (
    id          serial PRIMARY KEY,
    team_id     int NOT NULL REFERENCES teams(id),
    user_id     int NOT NULL REFERENCES users(id),
    status      text NOT NULL,            -- 'pending', 'confirmed', 'shipped', 'delivered', 'cancelled'
    total       numeric(12,2) NOT NULL,
    created_at  timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL
);

CREATE TABLE line_items (
    id          serial PRIMARY KEY,
    order_id    int NOT NULL REFERENCES orders(id),
    product_id  int NOT NULL REFERENCES products(id),
    quantity    int NOT NULL,
    unit_price  numeric(10,2) NOT NULL,
    total       numeric(12,2) NOT NULL
);

CREATE TABLE events (
    id          bigserial PRIMARY KEY,
    user_id     int NOT NULL REFERENCES users(id),
    event_type  text NOT NULL,            -- 'page_view', 'click', 'signup', 'purchase', 'api_call', 'error', 'logout'
    properties  jsonb,
    created_at  timestamptz NOT NULL
);

-- ════════════════════════════════════════════════════════════════════
--  Indexes (what a competent team would add, not exhaustive)
-- ════════════════════════════════════════════════════════════════════

CREATE INDEX idx_users_team_id       ON users(team_id);
CREATE INDEX idx_users_created_at    ON users(created_at);
CREATE INDEX idx_orders_team_id      ON orders(team_id);
CREATE INDEX idx_orders_user_id      ON orders(user_id);
CREATE INDEX idx_orders_status       ON orders(status);
CREATE INDEX idx_orders_created_at   ON orders(created_at);
CREATE INDEX idx_orders_status_user  ON orders(status, user_id);
CREATE INDEX idx_orders_status_team  ON orders(status, team_id);
CREATE INDEX idx_line_items_order_id ON line_items(order_id);
CREATE INDEX idx_line_items_product  ON line_items(product_id);
CREATE INDEX idx_events_user_id      ON events(user_id);
CREATE INDEX idx_events_type         ON events(event_type);
CREATE INDEX idx_events_created_at   ON events(created_at);
CREATE INDEX idx_events_props        ON events USING gin(properties);

COMMIT;

-- ════════════════════════════════════════════════════════════════════
--  Data generation (outside transaction for speed)
-- ════════════════════════════════════════════════════════════════════

\echo 'Generating teams (10,000)...'
INSERT INTO teams (name, plan, created_at, country)
SELECT
    'Team ' || i,
    (ARRAY['free','starter','pro','enterprise'])[1 + (random()*3)::int],
    '2020-01-01'::timestamptz + (random() * 1500)::int * interval '1 day',
    (ARRAY['US','GB','DE','FR','JP','BR','IN','CA','AU','KR'])[1 + (random()*9)::int]
FROM generate_series(1, 10000) AS i;

\echo 'Generating products (200)...'
INSERT INTO products (name, category, unit_price, is_active, created_at)
SELECT
    'Product ' || i,
    (ARRAY['compute','storage','network','support','addon'])[1 + (random()*4)::int],
    (5 + random() * 995)::numeric(10,2),
    random() > 0.1,
    '2020-01-01'::timestamptz + (random() * 365)::int * interval '1 day'
FROM generate_series(1, 200) AS i;

\echo 'Generating users (200,000)...'
INSERT INTO users (team_id, email, role, is_active, created_at, last_login)
SELECT
    1 + (random() * 9999)::int,
    'user' || i || '@example.com',
    (ARRAY['admin','member','member','member','viewer'])[1 + (random()*4)::int],
    random() > 0.15,
    '2020-01-01'::timestamptz + (random() * 1800)::int * interval '1 day',
    CASE WHEN random() > 0.3
         THEN '2024-01-01'::timestamptz + (random() * 450)::int * interval '1 day'
         ELSE NULL
    END
FROM generate_series(1, 200000) AS i;

\echo 'Generating orders (2,000,000)...'
INSERT INTO orders (team_id, user_id, status, total, created_at, updated_at)
SELECT
    u.team_id,
    u.id,
    (ARRAY['pending','confirmed','confirmed','shipped','delivered','delivered','delivered','cancelled'])[1 + (random()*7)::int],
    (10 + random() * 5000)::numeric(12,2),
    u.created_at + (random() * 600)::int * interval '1 day',
    u.created_at + (random() * 600)::int * interval '1 day' + (random() * 30)::int * interval '1 day'
FROM users u,
     generate_series(1, 10) AS s  -- ~10 orders per user
WHERE random() < 1.0  -- all users get orders
LIMIT 2000000;

\echo 'Generating line_items (8,000,000)...'
INSERT INTO line_items (order_id, product_id, quantity, unit_price, total)
SELECT
    o.id,
    1 + (random() * 199)::int,
    1 + (random() * 9)::int,
    p_price,
    (1 + (random() * 9)::int) * p_price
FROM orders o,
     generate_series(1, 4) AS s,  -- ~4 items per order
     LATERAL (SELECT (5 + random() * 995)::numeric(10,2) AS p_price) AS p
WHERE random() < 1.0
LIMIT 8000000;

\echo 'Generating events (10,000,000)...'
INSERT INTO events (user_id, event_type, properties, created_at)
SELECT
    1 + (random() * 199999)::int,
    (ARRAY['page_view','page_view','page_view','click','click','signup','purchase','api_call','api_call','error','logout'])[1 + (random()*10)::int],
    jsonb_build_object(
        'path', '/' || (ARRAY['home','dashboard','settings','billing','docs','api','admin','profile','search','reports'])[1 + (random()*9)::int],
        'duration_ms', (50 + random() * 5000)::int,
        'status', (ARRAY['200','200','200','200','301','400','404','500'])[1 + (random()*7)::int]
    ),
    '2024-01-01'::timestamptz + (random() * 450 * 24 * 3600)::int * interval '1 second'
FROM generate_series(1, 10000000) AS i;

-- ════════════════════════════════════════════════════════════════════
--  Analyze for stats
-- ════════════════════════════════════════════════════════════════════

\echo 'Analyzing tables...'
ANALYZE teams;
ANALYZE users;
ANALYZE products;
ANALYZE orders;
ANALYZE line_items;
ANALYZE events;

\echo 'Done. Table sizes:'
SELECT relname, pg_size_pretty(pg_total_relation_size(oid))
FROM pg_class
WHERE relname IN ('teams','users','products','orders','line_items','events')
ORDER BY pg_total_relation_size(oid) DESC;
