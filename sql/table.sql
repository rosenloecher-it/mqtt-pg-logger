-- The content of this file is parsed into commands by a quite simple algorithm. So please don't use ";" in comments


-- DROP TABLE JOURNAL;  -- do it manually. the automatic creation will abort/fail here.

CREATE TABLE journal (
    journal_id SERIAL PRIMARY KEY,

    topic VARCHAR(256),
    text VARCHAR(4096),
    data JSONB,

    message_id INTEGER,
    qos INTEGER,
    retain INTEGER,

    time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);


COMMENT ON COLUMN journal.journal_id is 'Primary key';
COMMENT ON COLUMN journal.message_id is 'Client message id (mid).';
COMMENT ON COLUMN journal.text is 'Message payload as standard text';
COMMENT ON COLUMN journal.data is 'JSON representation (generated out of "text" if not explicitly provided)';
COMMENT ON COLUMN journal.qos is 'Message quality of service 0, 1 or 2.';
COMMENT ON COLUMN journal.retain is 'If 1, the message is a retained message.';
COMMENT ON COLUMN journal.topic is 'Message topic.';
COMMENT ON COLUMN journal.time is 'Message or insert time';


-- used for regular clean up
CREATE INDEX CONCURRENTLY journal_time_idx ON journal ( time );

CREATE INDEX CONCURRENTLY journal_name_idx ON journal ( topic );


-- manual test
-- INSERT INTO JOURNAL (message_id, topic, text, qos, retain) values (1, 'topic', '{"a": "json"}', 1, 0);
-- SELECT * FROM journal;
