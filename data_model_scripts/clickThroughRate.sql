CREATE TABLE IF NOT EXISTS "Akalamio_TakeHome".click_through_rate (
    customer_id INT NOT NULL,
    event_id text,
    event_count_page_load int,
    event_count_click int,
    click_through_rate FLOAT DEFAULT 0,
    PRIMARY KEY (customer_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_click_through_rate_customer_hour ON "Akalamio_TakeHome".click_through_rate (customer_id, event_id);