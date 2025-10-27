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

/* Dataset */
CREATE TABLE dataset (
    id INTEGER NOT NULL PRIMARY KEY,
    uid VARCHAR(20) NOT NULL,
    name VARCHAR(1024) NOT NULL,
    resource_type VARCHAR(255) NOT NULL,
    data_provider VARCHAR(1024),
    institution VARCHAR(1024),
    date_created DATE,
    data_currency DATE,
    records INTEGER,
    media INTEGER
);

CREATE TABLE dataset_snapshot (
    id SERIAL PRIMARY KEY,
    uid VARCHAR(20) NOT NULL,
    snapshot_date DATE NOT NULL,
    records INTEGER,
    media INTEGER
);
CREATE UNIQUE INDEX dataset_snapshot_uid_snapshot_date_idx ON dataset_snapshot (uid, snapshot_date);

/* Spatial task */
CREATE TABLE spatial_task (
    id INTEGER NOT NULL PRIMARY KEY,
    created TIMESTAMP NOT NULL,
    name VARCHAR(40) NOT NULL,
    message TEXT,
    user_id INTEGER
);
CREATE INDEX spatial_task_created_idx ON spatial_task (created);
