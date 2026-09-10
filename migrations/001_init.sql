-- Миграция: таблица опросов и их результатов.
-- Применяется автоматически сервисом результатов при старте (idempotent).

CREATE TABLE IF NOT EXISTS polls (
    id         TEXT        PRIMARY KEY,
    question   TEXT        NOT NULL,
    options    JSONB       NOT NULL,
    status     TEXT        NOT NULL DEFAULT 'active',
    results    JSONB       NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    ends_at    TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS polls_status_idx ON polls (status);
CREATE INDEX IF NOT EXISTS polls_ends_at_idx ON polls (ends_at);
