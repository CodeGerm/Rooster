CREATE TABLE IF NOT EXISTS Event
(
    tid          INTEGER NOT NULL,
    uid          VARCHAR NOT NULL,
    event_time   BIGINT  NOT NULL,
    receipt_time BIGINT  NOT NULL,
    name         VARCHAR,
    message      VARCHAR,
    version      INTEGER,
CONSTRAINT Event_PK PRIMARY KEY (tid, uid, event_time, receipt_time))
COMPRESSION='SNAPPY';
