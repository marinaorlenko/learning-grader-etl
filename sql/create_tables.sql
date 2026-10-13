-- Основная таблица ETL: хранит попытки пользователей учебной платформы.
-- Каждая строка соответствует одному запуску кода (run)
-- или отправке решения на проверку (submit).
CREATE TABLE IF NOT EXISTS grader_attempts (
    user_id TEXT NOT NULL,
    oauth_consumer_key TEXT,
    lis_result_sourcedid TEXT,
    lis_outcome_service_url TEXT,
    is_correct BOOLEAN,
    attempt_type TEXT NOT NULL
        CHECK (attempt_type IN ('run', 'submit')),
    created_at TIMESTAMP NOT NULL
);

-- Уникальный индекс предотвращает повторную загрузку одинаковых попыток.
-- Он используется совместно с ON CONFLICT DO NOTHING в модуле загрузки.
-- NULLS NOT DISTINCT означает, что NULL считается равным NULL
-- при проверке уникальности. Это важно для записей с незаполненными
-- параметрами LTI или результатом проверки (is_correct).
CREATE UNIQUE INDEX IF NOT EXISTS grader_attempts_unique_idx
ON grader_attempts (
    user_id,
    oauth_consumer_key,
    lis_result_sourcedid,
    lis_outcome_service_url,
    is_correct,
    attempt_type,
    created_at
)
NULLS NOT DISTINCT;

-- Состояние ETL: хранит конец последнего непрерывного диапазона
-- успешно загруженных периодов для каждого пайплайна.
-- Используется для возобновления загрузки после прерывания или ошибки.
CREATE TABLE IF NOT EXISTS etl_state (
    pipeline_name TEXT PRIMARY KEY,
    last_successful_end TIMESTAMPTZ NOT NULL
);

-- Журнал успешно обработанных периодов.
-- Позволяет пропускать уже загруженные интервалы при повторном запуске,
-- в том числе если они находятся после временно пропущенного периода.
CREATE TABLE IF NOT EXISTS etl_periods (
    pipeline_name TEXT NOT NULL,
    period_start TIMESTAMPTZ NOT NULL,
    period_end TIMESTAMPTZ NOT NULL,
    -- Время фиксации успешной загрузки периода в PostgreSQL.
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Один и тот же период нельзя зарегистрировать повторно
    -- для одного пайплайна.
    PRIMARY KEY (pipeline_name, period_start, period_end),

    -- Начало периода должно предшествовать его окончанию.
    CHECK (period_start < period_end)
);
