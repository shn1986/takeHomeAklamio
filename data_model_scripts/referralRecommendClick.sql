CREATE TABLE IF NOT EXISTS "Akalamio_TakeHome".clicks (
    customer_id INT NOT NULL,
    date_hour   TIMESTAMP NOT NULL,
    clicks INT DEFAULT 0,
    PRIMARY KEY (customer_id, date_hour)
);

CREATE INDEX IF NOT EXISTS idx_clicks_customer_hour ON "Akalamio_TakeHome".clicks (customer_id, date_hour);