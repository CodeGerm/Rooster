CREATE TABLE IF NOT EXISTS Event
(
    tid          VARCHAR NOT NULL,
    uid          VARCHAR NOT NULL,
    event_time   TIMESTAMP,
    receipt_time TIMESTAMP,
    name         VARCHAR,
    message      VARCHAR,
    version      INTEGER,
    status       VARCHAR,
    context      VARCHAR,
CONSTRAINT Event_PK PRIMARY KEY (tid, uid, event_time, receipt_time)
COMPRESSION='SNAPPY';