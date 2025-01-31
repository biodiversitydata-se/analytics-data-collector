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
CREATE INDEX download_created_idx ON download (created);

CREATE TABLE "user" (
    id INTEGER NOT NULL PRIMARY KEY,
    user_key VARCHAR(255) NOT NULL,
    date_created TIMESTAMP NOT NULL,
    last_updated TIMESTAMP,
    last_login TIMESTAMP,
    country VARCHAR(255),
    organisation VARCHAR(255)
);
CREATE INDEX user_date_created_idx ON "user" (date_created);
CREATE INDEX user_last_login_idx ON "user" (last_login);
