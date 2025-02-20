/* Download */
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

/* User */
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

/* Visit */
CREATE TABLE visit (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    site_id INTEGER NOT NULL,
    unique_visitors INTEGER,
    visits INTEGER,
    actions INTEGER,
    bounce_count INTEGER,
    sum_visit_length INTEGER,
    pageviews INTEGER,
    unique_pageviews INTEGER,
    downloads INTEGER,
    unique_downloads INTEGER,
    outlinks INTEGER,
    unique_outlinks INTEGER,
    searches INTEGER,
    keywords INTEGER
);
CREATE UNIQUE INDEX visit_date_site_id_idx ON visit (date, site_id);

CREATE TABLE site (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(255)
);

/* Mirroreum */
CREATE TABLE mirroreum_login (
    id SERIAL PRIMARY KEY,
    login_at TIMESTAMP NOT NULL,
    ip VARCHAR(16) NOT NULL,
    is_test BOOLEAN
);
CREATE UNIQUE INDEX mirroreum_login_ip_login_at_idx ON mirroreum_login (ip, login_at);
