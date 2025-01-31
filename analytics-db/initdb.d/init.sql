CREATE TABLE download (
    id INTEGER NOT NULL PRIMARY KEY,
    created TIMESTAMP NOT NULL,
    reason VARCHAR(255),
    source VARCHAR(255),
    client VARCHAR(255),
    is_test BOOLEAN,
    user_key VARCHAR(255),
    user_agent VARCHAR(255)
);
CREATE INDEX created_idx ON download (created);

CREATE TABLE sbdi_user (
    id INTEGER NOT NULL PRIMARY KEY,
    user_key VARCHAR(255) NOT NULL,
    date_created TIMESTAMP NOT NULL,
    last_updated TIMESTAMP,
    last_login TIMESTAMP,
    country VARCHAR(255),
    organisation VARCHAR(255)
);
CREATE INDEX date_created_idx ON sbdi_user (date_created);
CREATE INDEX last_login_idx ON sbdi_user (last_login);
