-- =============================================================================
-- create_tables.sql
-- Creates the three core tables for the e-commerce ETL pipeline.
-- Safe to re-run: all statements use IF NOT EXISTS.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. pipeline_runs  (created first — products_market has a FK to it)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id            UUID          PRIMARY KEY,
    dag_id            VARCHAR(200)  NOT NULL,
    execution_date    DATE          NOT NULL,
    status            VARCHAR(20)   NOT NULL
                                    CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'PARTIAL')),
    rows_extracted    INTEGER       DEFAULT 0,
    rows_transformed  INTEGER       DEFAULT 0,
    rows_rejected     INTEGER       DEFAULT 0,
    rows_loaded       INTEGER       DEFAULT 0,
    data_quality_score NUMERIC(5,2) DEFAULT NULL,  -- rows_loaded / rows_extracted * 100
    started_at        TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMPTZ   DEFAULT NULL,
    error_message     TEXT          DEFAULT NULL
);

-- Prevent two DAG runs logging for the same logical execution_date
CREATE UNIQUE INDEX IF NOT EXISTS uq_pipeline_runs_dag_date
    ON pipeline_runs (dag_id, execution_date);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_date
    ON pipeline_runs (execution_date DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_status
    ON pipeline_runs (status);


-- ---------------------------------------------------------------------------
-- 2. products_market  (main fact table)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS products_market (
    id              SERIAL        PRIMARY KEY,

    -- Business key from the data source
    product_id      VARCHAR(100)  NOT NULL,
    name            VARCHAR(500)  NOT NULL,
    category        VARCHAR(100)  DEFAULT NULL,  -- normalised slug: men_clothing
    price           NUMERIC(12,2) NOT NULL CHECK (price > 0),
    rating          NUMERIC(3,2)  DEFAULT NULL CHECK (rating >= 0 AND rating <= 5),
    review_count    INTEGER       DEFAULT 0,
    stock_status    VARCHAR(50)   DEFAULT 'unknown',

    -- Provenance
    source          VARCHAR(100)  NOT NULL,   -- e.g. 'fakestore_api', 'tiki'
    source_url      TEXT          DEFAULT NULL,
    extraction_date DATE          NOT NULL,   -- logical partition key (Airflow ds)

    -- Traceability
    pipeline_run_id UUID          REFERENCES pipeline_runs (run_id) ON DELETE SET NULL,

    -- Housekeeping
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- UPSERT deduplication key: same product from the same source on the same day
-- ON CONFLICT (product_id, source, extraction_date) DO UPDATE ...
CREATE UNIQUE INDEX IF NOT EXISTS uq_products_market_key
    ON products_market (product_id, source, extraction_date);

CREATE INDEX IF NOT EXISTS idx_products_category
    ON products_market (category);

CREATE INDEX IF NOT EXISTS idx_products_extraction_date
    ON products_market (extraction_date DESC);

CREATE INDEX IF NOT EXISTS idx_products_source
    ON products_market (source);


-- ---------------------------------------------------------------------------
-- 3. rejected_records  (audit trail for every row dropped during transform)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS rejected_records (
    id               SERIAL       PRIMARY KEY,
    run_id           UUID         REFERENCES pipeline_runs (run_id) ON DELETE CASCADE,
    raw_data         JSONB        NOT NULL,        -- original row exactly as extracted
    rejection_reason VARCHAR(200) NOT NULL,        -- e.g. 'null_price', 'invalid_rating'
    rejected_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rejected_run_id
    ON rejected_records (run_id);

CREATE INDEX IF NOT EXISTS idx_rejected_reason
    ON rejected_records (rejection_reason);


-- ---------------------------------------------------------------------------
-- Confirmation output (visible in docker logs / psql)
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    RAISE NOTICE '✅  Schema initialised: pipeline_runs, products_market, rejected_records';
END
$$;