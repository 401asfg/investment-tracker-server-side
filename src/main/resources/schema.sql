CREATE TABLE IF NOT EXISTS vehicles (
    id INT NOT NULL AUTO_INCREMENT,
    symbol VARCHAR(10) NOT NULL,
    `name` VARCHAR(255) NOT NULL,

    PRIMARY KEY (id),
    UNIQUE (symbol),
    UNIQUE (`name`)
);

CREATE TABLE IF NOT EXISTS past_prices (
    id INT NOT NULL AUTO_INCREMENT,
    date_time TIMESTAMP NOT NULL,
    price FLOAT NOT NULL,
    is_closing BIT NOT NULL,
    vehicle_id INT NOT NULL,

    PRIMARY KEY (id),
    FOREIGN KEY (vehicle_id)
        REFERENCES vehicles (id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS portfolios (
    id INT NOT NULL AUTO_INCREMENT,
    usd_to_base_currency_rate_vehicle_id INT NOT NULL,

    PRIMARY KEY (id),
    FOREIGN KEY (usd_to_base_currency_rate_vehicle_id)
        REFERENCES vehicles (id)
);

CREATE TABLE IF NOT EXISTS investments (
    id INT NOT NULL AUTO_INCREMENT,
    date_time TIMESTAMP NOT NULL,
    principal FLOAT NOT NULL,
    vehicle_id INT NOT NULL,
    portfolio_id INT NOT NULL,

    PRIMARY KEY (id),
    FOREIGN KEY (vehicle_id)
        REFERENCES vehicles (id),
    FOREIGN KEY (portfolio_id)
        REFERENCES portfolios (id)
        ON DELETE CASCADE
);
