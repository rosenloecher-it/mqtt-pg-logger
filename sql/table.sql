-- The content of this file is parsed into commands by a quite simple algorithm. So please don't user ";" in comments

-- drop table journal;

create table journal (
    journal_id serial primary key,

    message_id integer,
    text varchar(1024),
    data jsonb,

    qos integer,
    retain integer,
    topic varchar(256),

    time timestamp with time zone default current_timestamp
);

comment on column journal.journal_id is 'Primary key';
comment on column journal.message_id is 'Client message id (mid).';
comment on column journal.text is 'Text payload';
comment on column journal.data is 'JSON representation (of "text")';
comment on column journal.qos is 'Message quality of service 0, 1 or 2.';
comment on column journal.retain is 'If 1, the message is a retained message and not fresh (otherwise 0).';
comment on column journal.topic is 'Topic that the message was published on.';
comment on column journal.time is 'Time the item was inserted into database.';

-- insert into journal (message_num, payload, qos, retain, topic) values (1, 'payload', 1, 0, 'topic');
-- select * from journal;


