-- The content of this file is parsed into commands by a quite simple algorithm. So please don't user ";" in comments


-- DROP TABLE JOURNAL;

CREATE TABLE journal (
    journal_id SERIAL PRIMARY KEY,

    topic VARCHAR(256),
    text VARCHAR(1024),
    data JSONB,

    message_id INTEGER,
    qos INTEGER,
    retain INTEGER,

    time TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
);


COMMENT ON COLUMN journal.journal_id is 'Primary key';
COMMENT ON COLUMN journal.message_id is 'Client message id (mid).';
COMMENT ON COLUMN journal.text is 'Text payload';
COMMENT ON COLUMN journal.data is 'JSON representation (of "text")';
COMMENT ON COLUMN journal.qos is 'Message quality of service 0, 1 or 2.';
COMMENT ON COLUMN journal.retain is 'If 1, the message is a retained message and not fresh (otherwise 0).';
COMMENT ON COLUMN journal.topic is 'Topic that the message was published on.';
COMMENT ON COLUMN journal.time is 'Time the item was inserted into database.';

-- INSERT INTO JOURNAL (message_id, topic, text, qos, retain) values (1, 'topic', '{"a": "json"}', 1, 0);
-- SELECT * FROM journal;


