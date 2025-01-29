CREATE TABLE downloads (
    id INTEGER NOT NULL PRIMARY KEY,
    created TIMESTAMP NOT NULL,
    reason VARCHAR(255),
    source VARCHAR(255),
    client VARCHAR(255),
    is_test BOOLEAN,
    user_key VARCHAR(255),
    user_agent VARCHAR(255)
);
CREATE INDEX created_idx ON downloads (created);
