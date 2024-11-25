CREATE TABLE IF NOT EXISTS "Akalamio_TakeHome".page_loads (
    customer_id INT NOT NULL,
    date_hour TIMESTAMP NOT NULL,
    page_loads INT DEFAULT 0,
    PRIMARY KEY (customer_id, date_hour)
);

CREATE INDEX IF NOT EXISTS idx_page_loads_customer_hour ON "Akalamio_TakeHome".page_loads (customer_id, date_hour);