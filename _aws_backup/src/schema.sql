-- ============================================================================
-- Books ETL Pipeline - PostgreSQL Database Schema
-- Version: 1.0
-- PostgreSQL: 16.1+
-- Purpose: Structured data storage with Change Data Capture (CDC)
-- ============================================================================

-- Drop existing schema (CAUTION: Only for initial setup/testing)
-- Uncomment the following lines to reset the database
-- DROP SCHEMA IF EXISTS public CASCADE;
-- CREATE SCHEMA public;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Currency dimension for flexible exchange rate management
CREATE TABLE dim_currency (
    currency_id SERIAL PRIMARY KEY,
    currency_code VARCHAR(3) NOT NULL UNIQUE,
    currency_name VARCHAR(50) NOT NULL,
    is_base_currency BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_currency_code_format CHECK (currency_code ~ '^[A-Z]{3}$')
);

COMMENT ON TABLE dim_currency IS 'Currency dimension table for multi-currency support';
COMMENT ON COLUMN dim_currency.is_base_currency IS 'True for GBP (base currency from source)';

-- Insert initial currencies
INSERT INTO dim_currency (currency_code, currency_name, is_base_currency) VALUES
    ('GBP', 'British Pound Sterling', TRUE),
    ('USD', 'US Dollar', FALSE),
    ('EUR', 'Euro', FALSE)
ON CONFLICT (currency_code) DO NOTHING;

-- Exchange rates table (allows for dynamic rate updates)
CREATE TABLE dim_exchange_rates (
    rate_id SERIAL PRIMARY KEY,
    from_currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
    to_currency_id INT NOT NULL REFERENCES dim_currency(currency_id),
    exchange_rate NUMERIC(10, 6) NOT NULL,
    effective_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT unique_rate_date UNIQUE (from_currency_id, to_currency_id, effective_date),
    CONSTRAINT chk_positive_rate CHECK (exchange_rate > 0),
    CONSTRAINT chk_different_currencies CHECK (from_currency_id != to_currency_id)
);

CREATE INDEX idx_exchange_rates_effective ON dim_exchange_rates(effective_date DESC);

COMMENT ON TABLE dim_exchange_rates IS 'Historical exchange rates for currency conversion';

-- Insert current exchange rates from transform_lambda.py
INSERT INTO dim_exchange_rates (from_currency_id, to_currency_id, exchange_rate, effective_date)
SELECT
    gbp.currency_id, usd.currency_id, 1.27, CURRENT_DATE
FROM dim_currency gbp, dim_currency usd
WHERE gbp.currency_code = 'GBP' AND usd.currency_code = 'USD'
ON CONFLICT (from_currency_id, to_currency_id, effective_date) DO NOTHING;

INSERT INTO dim_exchange_rates (from_currency_id, to_currency_id, exchange_rate, effective_date)
SELECT
    gbp.currency_id, eur.currency_id, 1.17, CURRENT_DATE
FROM dim_currency gbp, dim_currency eur
WHERE gbp.currency_code = 'GBP' AND eur.currency_code = 'EUR'
ON CONFLICT (from_currency_id, to_currency_id, effective_date) DO NOTHING;

-- Date dimension for time-series analysis
CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date_actual DATE NOT NULL UNIQUE,
    day_of_week INT NOT NULL CHECK (day_of_week BETWEEN 0 AND 6),
    day_name VARCHAR(10) NOT NULL,
    day_of_month INT NOT NULL CHECK (day_of_month BETWEEN 1 AND 31),
    week_of_year INT NOT NULL CHECK (week_of_year BETWEEN 1 AND 53),
    month_number INT NOT NULL CHECK (month_number BETWEEN 1 AND 12),
    month_name VARCHAR(10) NOT NULL,
    quarter INT NOT NULL CHECK (quarter BETWEEN 1 AND 4),
    year INT NOT NULL CHECK (year BETWEEN 2000 AND 2100),
    is_weekend BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_date_actual ON dim_date(date_actual);
CREATE INDEX idx_dim_date_year_month ON dim_date(year, month_number);

COMMENT ON TABLE dim_date IS 'Date dimension for time-series analysis and reporting';

-- Function to populate date dimension
CREATE OR REPLACE FUNCTION populate_dim_date(start_date DATE, end_date DATE)
RETURNS void AS $$
BEGIN
    INSERT INTO dim_date (
        date_actual, day_of_week, day_name, day_of_month,
        week_of_year, month_number, month_name, quarter, year, is_weekend
    )
    SELECT
        d::DATE,
        EXTRACT(DOW FROM d)::INT,
        TRIM(TO_CHAR(d, 'Day')),
        EXTRACT(DAY FROM d)::INT,
        EXTRACT(WEEK FROM d)::INT,
        EXTRACT(MONTH FROM d)::INT,
        TRIM(TO_CHAR(d, 'Month')),
        EXTRACT(QUARTER FROM d)::INT,
        EXTRACT(YEAR FROM d)::INT,
        EXTRACT(DOW FROM d) IN (0, 6)
    FROM generate_series(start_date, end_date, '1 day'::interval) d
    ON CONFLICT (date_actual) DO NOTHING;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION populate_dim_date IS 'Populate date dimension for a given date range';

-- Populate for 2026 and 2027
SELECT populate_dim_date('2026-01-01', '2027-12-31');

-- ============================================================================
-- BOOK MASTER TABLE (SCD TYPE 2)
-- ============================================================================

-- Master book table with slowly changing dimensions
CREATE TABLE books (
    book_id SERIAL PRIMARY KEY,

    -- Natural key and identity
    title VARCHAR(500) NOT NULL,
    book_hash VARCHAR(64) NOT NULL,  -- SHA256 of title for fast lookups

    -- Pricing information (all currencies)
    price_gbp NUMERIC(10, 2) NOT NULL,
    price_usd NUMERIC(10, 2),
    price_eur NUMERIC(10, 2),

    -- Availability tracking
    availability_status VARCHAR(100) NOT NULL,
    is_in_stock BOOLEAN NOT NULL,
    stock_quantity INT,  -- Future enhancement: parse "In stock (15 available)"

    -- SCD Type 2 tracking
    valid_from DATE NOT NULL,
    valid_to DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,

    -- Change tracking
    change_type VARCHAR(20),  -- 'NEW', 'PRICE_CHANGE', 'STOCK_CHANGE', 'BOTH'
    previous_price_gbp NUMERIC(10, 2),
    previous_availability_status VARCHAR(100),

    -- Audit fields
    scraped_at TIMESTAMP NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_s3_key VARCHAR(500),

    -- Constraints
    CONSTRAINT chk_valid_dates CHECK (valid_from <= valid_to),
    CONSTRAINT chk_price_positive CHECK (price_gbp > 0),
    CONSTRAINT chk_title_not_empty CHECK (LENGTH(TRIM(title)) > 0),
    CONSTRAINT chk_change_type CHECK (change_type IN ('NEW', 'PRICE_CHANGE', 'STOCK_CHANGE', 'BOTH'))
);

-- Indexes for performance
CREATE INDEX idx_books_title ON books(title);
CREATE INDEX idx_books_hash ON books(book_hash);
CREATE INDEX idx_books_is_current ON books(is_current);
CREATE INDEX idx_books_valid_from ON books(valid_from);
CREATE INDEX idx_books_valid_to ON books(valid_to);
CREATE INDEX idx_books_composite ON books(book_hash, is_current) WHERE is_current = TRUE;
CREATE INDEX idx_books_scraped_date ON books(DATE(scraped_at));

-- Unique constraint for current records
CREATE UNIQUE INDEX idx_books_unique_current
ON books(book_hash, valid_from)
WHERE is_current = TRUE;

COMMENT ON TABLE books IS 'SCD Type 2 table storing historical book data';
COMMENT ON COLUMN books.book_hash IS 'SHA256 hash of title for duplicate detection';
COMMENT ON COLUMN books.valid_from IS 'Start date of this record version';
COMMENT ON COLUMN books.valid_to IS 'End date of this record version (9999-12-31 for current)';
COMMENT ON COLUMN books.is_current IS 'True for the latest version of a book';

-- Function to generate book hash
CREATE OR REPLACE FUNCTION generate_book_hash(p_title TEXT)
RETURNS VARCHAR(64) AS $$
BEGIN
    RETURN encode(digest(lower(trim(p_title)), 'sha256'), 'hex');
END;
$$ LANGUAGE plpgsql IMMUTABLE;

COMMENT ON FUNCTION generate_book_hash IS 'Generate SHA256 hash of book title for duplicate detection';

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- Daily price facts for analytical queries
CREATE TABLE fact_daily_prices (
    price_fact_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    book_id INT NOT NULL REFERENCES books(book_id),

    -- Prices in all currencies
    price_gbp NUMERIC(10, 2) NOT NULL,
    price_usd NUMERIC(10, 2) NOT NULL,
    price_eur NUMERIC(10, 2) NOT NULL,

    -- Stock information
    is_in_stock BOOLEAN NOT NULL,

    -- Change indicators
    price_changed_from_previous BOOLEAN DEFAULT FALSE,
    price_change_amount_gbp NUMERIC(10, 2),
    price_change_percentage NUMERIC(5, 2),

    stock_changed_from_previous BOOLEAN DEFAULT FALSE,

    -- Audit
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT chk_prices_consistent CHECK (price_gbp > 0 AND price_usd > 0 AND price_eur > 0)
);

CREATE INDEX idx_fact_prices_date ON fact_daily_prices(date_id);
CREATE INDEX idx_fact_prices_book ON fact_daily_prices(book_id);
CREATE INDEX idx_fact_prices_date_book ON fact_daily_prices(date_id, book_id);

COMMENT ON TABLE fact_daily_prices IS 'Daily price snapshots for time-series analysis';

-- Daily summary facts
CREATE TABLE fact_daily_summary (
    summary_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL REFERENCES dim_date(date_id),

    -- Aggregated metrics
    total_books_scraped INT NOT NULL CHECK (total_books_scraped >= 0),
    total_books_in_stock INT NOT NULL CHECK (total_books_in_stock >= 0),
    total_books_out_of_stock INT NOT NULL CHECK (total_books_out_of_stock >= 0),

    -- Inventory values
    total_inventory_value_gbp NUMERIC(12, 2) NOT NULL CHECK (total_inventory_value_gbp >= 0),
    total_inventory_value_usd NUMERIC(12, 2) NOT NULL CHECK (total_inventory_value_usd >= 0),
    total_inventory_value_eur NUMERIC(12, 2) NOT NULL CHECK (total_inventory_value_eur >= 0),

    -- Average prices
    avg_price_gbp NUMERIC(10, 2) NOT NULL CHECK (avg_price_gbp >= 0),
    avg_price_usd NUMERIC(10, 2) NOT NULL CHECK (avg_price_usd >= 0),
    avg_price_eur NUMERIC(10, 2) NOT NULL CHECK (avg_price_eur >= 0),

    -- Change tracking
    new_books_added INT DEFAULT 0,
    books_removed INT DEFAULT 0,
    books_with_price_changes INT DEFAULT 0,
    books_with_stock_changes INT DEFAULT 0,

    -- Audit
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_s3_key VARCHAR(500),

    CONSTRAINT unique_summary_date UNIQUE (date_id)
);

CREATE INDEX idx_fact_summary_date ON fact_daily_summary(date_id);

COMMENT ON TABLE fact_daily_summary IS 'Daily aggregated metrics and statistics';

-- ============================================================================
-- CHANGE DATA CAPTURE (CDC) TABLES
-- ============================================================================

-- CDC event log for detailed change tracking
CREATE TABLE cdc_events (
    event_id SERIAL PRIMARY KEY,
    event_type VARCHAR(50) NOT NULL,  -- 'BOOK_ADDED', 'PRICE_CHANGE', 'STOCK_CHANGE', 'BOOK_REMOVED'
    book_id INT REFERENCES books(book_id),

    -- Change details
    title VARCHAR(500) NOT NULL,
    book_hash VARCHAR(64) NOT NULL,

    -- Old values
    old_price_gbp NUMERIC(10, 2),
    old_availability VARCHAR(100),
    old_is_in_stock BOOLEAN,

    -- New values
    new_price_gbp NUMERIC(10, 2),
    new_availability VARCHAR(100),
    new_is_in_stock BOOLEAN,

    -- Calculated changes
    price_change_amount NUMERIC(10, 2),
    price_change_percentage NUMERIC(5, 2),

    -- Metadata
    detected_at DATE NOT NULL,
    event_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- For correlation
    etl_batch_id VARCHAR(50),

    CONSTRAINT chk_event_type CHECK (event_type IN ('BOOK_ADDED', 'PRICE_CHANGE', 'STOCK_CHANGE', 'BOOK_REMOVED'))
);

CREATE INDEX idx_cdc_events_type ON cdc_events(event_type);
CREATE INDEX idx_cdc_events_date ON cdc_events(detected_at);
CREATE INDEX idx_cdc_events_book ON cdc_events(book_id);
CREATE INDEX idx_cdc_events_hash ON cdc_events(book_hash);
CREATE INDEX idx_cdc_events_timestamp ON cdc_events(event_timestamp DESC);

COMMENT ON TABLE cdc_events IS 'Change data capture event log for all detected changes';

-- ============================================================================
-- STAGING TABLE FOR ETL PROCESSING
-- ============================================================================

-- Staging table for incoming data before CDC processing
CREATE TABLE staging_books (
    staging_id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    price_gbp NUMERIC(10, 2) NOT NULL CHECK (price_gbp > 0),
    availability VARCHAR(100) NOT NULL,
    is_in_stock BOOLEAN NOT NULL,
    scraped_date DATE NOT NULL,
    batch_id VARCHAR(50) NOT NULL,
    processed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_staging_processed ON staging_books(processed);
CREATE INDEX idx_staging_batch ON staging_books(batch_id);
CREATE INDEX idx_staging_date ON staging_books(scraped_date);

COMMENT ON TABLE staging_books IS 'Staging table for incoming ETL data before CDC processing';

-- ============================================================================
-- VIEWS FOR ANALYTICAL QUERIES
-- ============================================================================

-- Current book catalog view
CREATE OR REPLACE VIEW v_current_books AS
SELECT
    b.book_id,
    b.title,
    b.price_gbp,
    b.price_usd,
    b.price_eur,
    b.availability_status,
    b.is_in_stock,
    b.valid_from,
    b.scraped_at
FROM books b
WHERE b.is_current = TRUE
ORDER BY b.title;

COMMENT ON VIEW v_current_books IS 'Current active book catalog';

-- Price change history view
CREATE OR REPLACE VIEW v_price_history AS
SELECT
    b.book_id,
    b.title,
    b.price_gbp,
    b.valid_from,
    b.valid_to,
    b.is_current,
    b.change_type,
    b.previous_price_gbp,
    CASE
        WHEN b.previous_price_gbp IS NOT NULL AND b.previous_price_gbp > 0
        THEN ROUND(((b.price_gbp - b.previous_price_gbp) / b.previous_price_gbp * 100), 2)
        ELSE NULL
    END AS price_change_percentage
FROM books b
ORDER BY b.title, b.valid_from;

COMMENT ON VIEW v_price_history IS 'Complete price change history for all books';

-- Daily price trends
CREATE OR REPLACE VIEW v_daily_price_trends AS
SELECT
    dd.date_actual,
    dd.day_name,
    dd.year,
    dd.month_name,
    COUNT(DISTINCT fp.book_id) AS total_books,
    ROUND(AVG(fp.price_gbp), 2) AS avg_price_gbp,
    ROUND(MIN(fp.price_gbp), 2) AS min_price_gbp,
    ROUND(MAX(fp.price_gbp), 2) AS max_price_gbp,
    SUM(CASE WHEN fp.price_changed_from_previous THEN 1 ELSE 0 END) AS books_with_price_changes,
    SUM(CASE WHEN fp.is_in_stock THEN 1 ELSE 0 END) AS books_in_stock
FROM fact_daily_prices fp
JOIN dim_date dd ON fp.date_id = dd.date_id
GROUP BY dd.date_actual, dd.day_name, dd.year, dd.month_name
ORDER BY dd.date_actual;

COMMENT ON VIEW v_daily_price_trends IS 'Daily aggregated price trends and statistics';

-- Stock availability trends
CREATE OR REPLACE VIEW v_stock_trends AS
SELECT
    dd.date_actual,
    dd.day_name,
    SUM(CASE WHEN fp.is_in_stock THEN 1 ELSE 0 END) AS in_stock_count,
    SUM(CASE WHEN NOT fp.is_in_stock THEN 1 ELSE 0 END) AS out_of_stock_count,
    COUNT(*) AS total_books,
    ROUND(AVG(CASE WHEN fp.is_in_stock THEN 1 ELSE 0 END) * 100, 2) AS in_stock_percentage
FROM fact_daily_prices fp
JOIN dim_date dd ON fp.date_id = dd.date_id
GROUP BY dd.date_actual, dd.day_name
ORDER BY dd.date_actual;

COMMENT ON VIEW v_stock_trends IS 'Daily stock availability trends';

-- CDC event summary
CREATE OR REPLACE VIEW v_cdc_summary AS
SELECT
    detected_at,
    event_type,
    COUNT(*) AS event_count,
    ROUND(AVG(price_change_percentage), 2) AS avg_price_change_pct,
    COUNT(DISTINCT book_hash) AS unique_books_affected
FROM cdc_events
GROUP BY detected_at, event_type
ORDER BY detected_at DESC, event_type;

COMMENT ON VIEW v_cdc_summary IS 'CDC event summary by date and event type';

-- Price volatility analysis
CREATE OR REPLACE VIEW v_price_volatility AS
SELECT
    b.title,
    COUNT(*) AS price_change_count,
    MIN(b.price_gbp) AS min_price,
    MAX(b.price_gbp) AS max_price,
    ROUND(AVG(b.price_gbp), 2) AS avg_price,
    ROUND(STDDEV(b.price_gbp), 2) AS price_stddev,
    ROUND(((MAX(b.price_gbp) - MIN(b.price_gbp)) / MIN(b.price_gbp) * 100), 2) AS price_range_pct
FROM books b
WHERE b.change_type IN ('PRICE_CHANGE', 'BOTH')
GROUP BY b.title
HAVING COUNT(*) > 1
ORDER BY price_change_count DESC, price_range_pct DESC;

COMMENT ON VIEW v_price_volatility IS 'Price volatility analysis showing books with frequent price changes';

-- ============================================================================
-- CDC PROCESSING STORED PROCEDURE
-- ============================================================================

CREATE OR REPLACE FUNCTION process_cdc_batch(p_batch_id VARCHAR(50), p_scraped_date DATE)
RETURNS TABLE(
    new_books INT,
    removed_books INT,
    price_changes INT,
    stock_changes INT,
    total_processed INT
) AS $$
DECLARE
    v_new_books INT := 0;
    v_removed_books INT := 0;
    v_price_changes INT := 0;
    v_stock_changes INT := 0;
    v_total_processed INT := 0;
    v_date_id INT;
    v_usd_rate NUMERIC(10, 6);
    v_eur_rate NUMERIC(10, 6);
BEGIN
    -- Get date_id for the scraped date
    SELECT date_id INTO v_date_id FROM dim_date WHERE date_actual = p_scraped_date;

    IF v_date_id IS NULL THEN
        RAISE EXCEPTION 'Date % not found in dim_date. Run populate_dim_date() first.', p_scraped_date;
    END IF;

    -- Get latest exchange rates
    SELECT exchange_rate INTO v_usd_rate
    FROM dim_exchange_rates
    WHERE from_currency_id = (SELECT currency_id FROM dim_currency WHERE currency_code = 'GBP')
    AND to_currency_id = (SELECT currency_id FROM dim_currency WHERE currency_code = 'USD')
    ORDER BY effective_date DESC
    LIMIT 1;

    SELECT exchange_rate INTO v_eur_rate
    FROM dim_exchange_rates
    WHERE from_currency_id = (SELECT currency_id FROM dim_currency WHERE currency_code = 'GBP')
    AND to_currency_id = (SELECT currency_id FROM dim_currency WHERE currency_code = 'EUR')
    ORDER BY effective_date DESC
    LIMIT 1;

    -- ========================================================================
    -- STEP 1: Identify NEW books
    -- ========================================================================
    INSERT INTO books (
        title, book_hash, price_gbp, price_usd, price_eur,
        availability_status, is_in_stock,
        valid_from, valid_to, is_current, change_type, scraped_at
    )
    SELECT
        s.title,
        generate_book_hash(s.title),
        s.price_gbp,
        ROUND(s.price_gbp * v_usd_rate, 2),
        ROUND(s.price_gbp * v_eur_rate, 2),
        s.availability,
        s.is_in_stock,
        p_scraped_date,
        '9999-12-31',
        TRUE,
        'NEW',
        CURRENT_TIMESTAMP
    FROM staging_books s
    WHERE s.batch_id = p_batch_id
    AND NOT EXISTS (
        SELECT 1 FROM books b
        WHERE b.book_hash = generate_book_hash(s.title)
    );

    GET DIAGNOSTICS v_new_books = ROW_COUNT;

    -- Log new book events
    INSERT INTO cdc_events (
        event_type, book_id, title, book_hash, new_price_gbp,
        new_availability, new_is_in_stock, detected_at, etl_batch_id
    )
    SELECT
        'BOOK_ADDED',
        b.book_id,
        b.title,
        b.book_hash,
        b.price_gbp,
        b.availability_status,
        b.is_in_stock,
        p_scraped_date,
        p_batch_id
    FROM books b
    WHERE b.change_type = 'NEW'
    AND b.valid_from = p_scraped_date;

    -- ========================================================================
    -- STEP 2: Detect PRICE and STOCK changes
    -- ========================================================================

    -- Close old records and insert new versions for books with changes
    WITH changed_books AS (
        SELECT
            b.book_id,
            b.title,
            b.book_hash,
            b.price_gbp AS old_price,
            b.availability_status AS old_availability,
            b.is_in_stock AS old_stock,
            s.price_gbp AS new_price,
            s.availability AS new_availability,
            s.is_in_stock AS new_stock,
            CASE
                WHEN b.price_gbp <> s.price_gbp AND b.is_in_stock <> s.is_in_stock THEN 'BOTH'
                WHEN b.price_gbp <> s.price_gbp THEN 'PRICE_CHANGE'
                WHEN b.is_in_stock <> s.is_in_stock THEN 'STOCK_CHANGE'
            END AS change_type
        FROM books b
        JOIN staging_books s ON b.book_hash = generate_book_hash(s.title)
        WHERE b.is_current = TRUE
        AND s.batch_id = p_batch_id
        AND (
            b.price_gbp <> s.price_gbp
            OR b.availability_status <> s.availability
            OR b.is_in_stock <> s.is_in_stock
        )
    )
    -- Update old records (close them)
    UPDATE books b
    SET
        valid_to = p_scraped_date - INTERVAL '1 day',
        is_current = FALSE
    FROM changed_books cb
    WHERE b.book_id = cb.book_id
    AND b.is_current = TRUE;

    -- Insert new versions
    WITH changed_books AS (
        SELECT
            b.book_id,
            b.title,
            b.book_hash,
            b.price_gbp AS old_price,
            b.availability_status AS old_availability,
            b.is_in_stock AS old_stock,
            s.price_gbp AS new_price,
            s.availability AS new_availability,
            s.is_in_stock AS new_stock,
            CASE
                WHEN b.price_gbp <> s.price_gbp AND b.is_in_stock <> s.is_in_stock THEN 'BOTH'
                WHEN b.price_gbp <> s.price_gbp THEN 'PRICE_CHANGE'
                WHEN b.is_in_stock <> s.is_in_stock THEN 'STOCK_CHANGE'
            END AS change_type
        FROM books b
        JOIN staging_books s ON b.book_hash = generate_book_hash(s.title)
        WHERE b.valid_to = p_scraped_date - INTERVAL '1 day'
        AND s.batch_id = p_batch_id
    )
    INSERT INTO books (
        title, book_hash, price_gbp, price_usd, price_eur,
        availability_status, is_in_stock,
        valid_from, valid_to, is_current, change_type,
        previous_price_gbp, previous_availability_status, scraped_at
    )
    SELECT
        cb.title,
        cb.book_hash,
        cb.new_price,
        ROUND(cb.new_price * v_usd_rate, 2),
        ROUND(cb.new_price * v_eur_rate, 2),
        cb.new_availability,
        cb.new_stock,
        p_scraped_date,
        '9999-12-31',
        TRUE,
        cb.change_type,
        cb.old_price,
        cb.old_availability,
        CURRENT_TIMESTAMP
    FROM changed_books cb;

    -- Count changes
    SELECT COUNT(*) INTO v_price_changes
    FROM books
    WHERE valid_from = p_scraped_date
    AND change_type IN ('PRICE_CHANGE', 'BOTH');

    SELECT COUNT(*) INTO v_stock_changes
    FROM books
    WHERE valid_from = p_scraped_date
    AND change_type IN ('STOCK_CHANGE', 'BOTH');

    -- Log CDC events for price changes
    INSERT INTO cdc_events (
        event_type, book_id, title, book_hash,
        old_price_gbp, old_availability, old_is_in_stock,
        new_price_gbp, new_availability, new_is_in_stock,
        price_change_amount, price_change_percentage,
        detected_at, etl_batch_id
    )
    SELECT
        'PRICE_CHANGE',
        b.book_id,
        b.title,
        b.book_hash,
        b.previous_price_gbp,
        b.previous_availability_status,
        CASE WHEN b.previous_availability_status LIKE '%In stock%' THEN TRUE ELSE FALSE END,
        b.price_gbp,
        b.availability_status,
        b.is_in_stock,
        b.price_gbp - b.previous_price_gbp,
        CASE
            WHEN b.previous_price_gbp > 0
            THEN ROUND(((b.price_gbp - b.previous_price_gbp) / b.previous_price_gbp * 100), 2)
            ELSE NULL
        END,
        p_scraped_date,
        p_batch_id
    FROM books b
    WHERE b.valid_from = p_scraped_date
    AND b.change_type IN ('PRICE_CHANGE', 'BOTH');

    -- Log stock change events separately
    INSERT INTO cdc_events (
        event_type, book_id, title, book_hash,
        old_availability, old_is_in_stock,
        new_availability, new_is_in_stock,
        detected_at, etl_batch_id
    )
    SELECT
        'STOCK_CHANGE',
        b.book_id,
        b.title,
        b.book_hash,
        b.previous_availability_status,
        CASE WHEN b.previous_availability_status LIKE '%In stock%' THEN TRUE ELSE FALSE END,
        b.availability_status,
        b.is_in_stock,
        p_scraped_date,
        p_batch_id
    FROM books b
    WHERE b.valid_from = p_scraped_date
    AND b.change_type IN ('STOCK_CHANGE', 'BOTH');

    -- ========================================================================
    -- STEP 3: Detect REMOVED books
    -- ========================================================================

    WITH removed_books AS (
        SELECT b.book_id, b.title, b.book_hash, b.price_gbp, b.availability_status, b.is_in_stock
        FROM books b
        WHERE b.is_current = TRUE
        AND NOT EXISTS (
            SELECT 1 FROM staging_books s
            WHERE s.batch_id = p_batch_id
            AND generate_book_hash(s.title) = b.book_hash
        )
    )
    UPDATE books b
    SET
        valid_to = p_scraped_date - INTERVAL '1 day',
        is_current = FALSE
    FROM removed_books rb
    WHERE b.book_id = rb.book_id;

    GET DIAGNOSTICS v_removed_books = ROW_COUNT;

    -- Log removed book events
    INSERT INTO cdc_events (
        event_type, book_id, title, book_hash,
        old_price_gbp, old_availability, old_is_in_stock,
        detected_at, etl_batch_id
    )
    SELECT
        'BOOK_REMOVED',
        rb.book_id,
        rb.title,
        rb.book_hash,
        rb.price_gbp,
        rb.availability_status,
        rb.is_in_stock,
        p_scraped_date,
        p_batch_id
    FROM (
        SELECT b.book_id, b.title, b.book_hash, b.price_gbp, b.availability_status, b.is_in_stock
        FROM books b
        WHERE b.valid_to = p_scraped_date - INTERVAL '1 day'
        AND NOT EXISTS (
            SELECT 1 FROM books b2
            WHERE b2.book_hash = b.book_hash
            AND b2.is_current = TRUE
        )
    ) rb;

    -- ========================================================================
    -- STEP 4: Populate fact_daily_prices
    -- ========================================================================

    INSERT INTO fact_daily_prices (
        date_id, book_id, price_gbp, price_usd, price_eur, is_in_stock,
        price_changed_from_previous, price_change_amount_gbp, price_change_percentage,
        stock_changed_from_previous
    )
    SELECT
        v_date_id,
        b.book_id,
        b.price_gbp,
        b.price_usd,
        b.price_eur,
        b.is_in_stock,
        CASE WHEN b.change_type IN ('PRICE_CHANGE', 'BOTH') THEN TRUE ELSE FALSE END,
        CASE WHEN b.previous_price_gbp IS NOT NULL THEN b.price_gbp - b.previous_price_gbp ELSE NULL END,
        CASE
            WHEN b.previous_price_gbp IS NOT NULL AND b.previous_price_gbp > 0
            THEN ROUND(((b.price_gbp - b.previous_price_gbp) / b.previous_price_gbp * 100), 2)
            ELSE NULL
        END,
        CASE WHEN b.change_type IN ('STOCK_CHANGE', 'BOTH') THEN TRUE ELSE FALSE END
    FROM books b
    WHERE b.is_current = TRUE;

    -- ========================================================================
    -- STEP 5: Populate fact_daily_summary
    -- ========================================================================

    INSERT INTO fact_daily_summary (
        date_id,
        total_books_scraped, total_books_in_stock, total_books_out_of_stock,
        total_inventory_value_gbp, total_inventory_value_usd, total_inventory_value_eur,
        avg_price_gbp, avg_price_usd, avg_price_eur,
        new_books_added, books_removed,
        books_with_price_changes, books_with_stock_changes
    )
    SELECT
        v_date_id,
        COUNT(*),
        SUM(CASE WHEN is_in_stock THEN 1 ELSE 0 END),
        SUM(CASE WHEN NOT is_in_stock THEN 1 ELSE 0 END),
        SUM(CASE WHEN is_in_stock THEN price_gbp ELSE 0 END),
        SUM(CASE WHEN is_in_stock THEN price_usd ELSE 0 END),
        SUM(CASE WHEN is_in_stock THEN price_eur ELSE 0 END),
        ROUND(AVG(price_gbp), 2),
        ROUND(AVG(price_usd), 2),
        ROUND(AVG(price_eur), 2),
        v_new_books,
        v_removed_books,
        v_price_changes,
        v_stock_changes
    FROM fact_daily_prices
    WHERE date_id = v_date_id
    ON CONFLICT (date_id) DO UPDATE SET
        total_books_scraped = EXCLUDED.total_books_scraped,
        total_books_in_stock = EXCLUDED.total_books_in_stock,
        total_books_out_of_stock = EXCLUDED.total_books_out_of_stock,
        total_inventory_value_gbp = EXCLUDED.total_inventory_value_gbp,
        total_inventory_value_usd = EXCLUDED.total_inventory_value_usd,
        total_inventory_value_eur = EXCLUDED.total_inventory_value_eur,
        avg_price_gbp = EXCLUDED.avg_price_gbp,
        avg_price_usd = EXCLUDED.avg_price_usd,
        avg_price_eur = EXCLUDED.avg_price_eur,
        new_books_added = EXCLUDED.new_books_added,
        books_removed = EXCLUDED.books_removed,
        books_with_price_changes = EXCLUDED.books_with_price_changes,
        books_with_stock_changes = EXCLUDED.books_with_stock_changes,
        processed_at = CURRENT_TIMESTAMP;

    -- ========================================================================
    -- STEP 6: Mark staging records as processed
    -- ========================================================================

    UPDATE staging_books
    SET processed = TRUE
    WHERE batch_id = p_batch_id;

    GET DIAGNOSTICS v_total_processed = ROW_COUNT;

    -- Return summary
    RETURN QUERY SELECT v_new_books, v_removed_books, v_price_changes, v_stock_changes, v_total_processed;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION process_cdc_batch IS 'Process CDC batch: detect changes, update SCD Type 2 records, log events, populate fact tables';

-- ============================================================================
-- DATA QUALITY AND MAINTENANCE
-- ============================================================================

-- Trigger to prevent direct updates to closed SCD records
CREATE OR REPLACE FUNCTION prevent_closed_record_updates()
RETURNS TRIGGER AS $$
BEGIN
    IF OLD.is_current = FALSE AND NEW.is_current = FALSE THEN
        RAISE EXCEPTION 'Cannot modify closed historical records (book_id: %, valid_to: %)', OLD.book_id, OLD.valid_to;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_closed_updates
    BEFORE UPDATE ON books
    FOR EACH ROW
    EXECUTE FUNCTION prevent_closed_record_updates();

COMMENT ON FUNCTION prevent_closed_record_updates IS 'Prevent modifications to closed SCD Type 2 records';

-- ============================================================================
-- UTILITY FUNCTIONS
-- ============================================================================

-- Function to get current exchange rate
CREATE OR REPLACE FUNCTION get_exchange_rate(
    p_from_currency VARCHAR(3),
    p_to_currency VARCHAR(3),
    p_effective_date DATE DEFAULT CURRENT_DATE
)
RETURNS NUMERIC(10, 6) AS $$
DECLARE
    v_rate NUMERIC(10, 6);
BEGIN
    SELECT exchange_rate INTO v_rate
    FROM dim_exchange_rates der
    JOIN dim_currency dc_from ON der.from_currency_id = dc_from.currency_id
    JOIN dim_currency dc_to ON der.to_currency_id = dc_to.currency_id
    WHERE dc_from.currency_code = p_from_currency
    AND dc_to.currency_code = p_to_currency
    AND der.effective_date <= p_effective_date
    ORDER BY der.effective_date DESC
    LIMIT 1;

    IF v_rate IS NULL THEN
        RAISE EXCEPTION 'No exchange rate found for % to % on %', p_from_currency, p_to_currency, p_effective_date;
    END IF;

    RETURN v_rate;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_exchange_rate IS 'Get exchange rate for a currency pair on a specific date';

-- ============================================================================
-- GRANTS (Adjust based on your security requirements)
-- ============================================================================

-- Grant read-only access to analytical views (example)
-- CREATE ROLE analytics_reader;
-- GRANT CONNECT ON DATABASE books_etl TO analytics_reader;
-- GRANT USAGE ON SCHEMA public TO analytics_reader;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_reader;
-- ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO analytics_reader;

-- ============================================================================
-- SCHEMA VALIDATION
-- ============================================================================

-- Verify schema creation
DO $$
DECLARE
    v_table_count INT;
    v_view_count INT;
    v_function_count INT;
BEGIN
    SELECT COUNT(*) INTO v_table_count
    FROM information_schema.tables
    WHERE table_schema = 'public'
    AND table_type = 'BASE TABLE';

    SELECT COUNT(*) INTO v_view_count
    FROM information_schema.views
    WHERE table_schema = 'public';

    SELECT COUNT(*) INTO v_function_count
    FROM information_schema.routines
    WHERE routine_schema = 'public'
    AND routine_type = 'FUNCTION';

    RAISE NOTICE '========================================';
    RAISE NOTICE 'Schema Validation Complete';
    RAISE NOTICE '========================================';
    RAISE NOTICE 'Tables created: %', v_table_count;
    RAISE NOTICE 'Views created: %', v_view_count;
    RAISE NOTICE 'Functions created: %', v_function_count;
    RAISE NOTICE '========================================';

    IF v_table_count < 8 THEN
        RAISE WARNING 'Expected at least 8 tables, found %', v_table_count;
    END IF;
END $$;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
