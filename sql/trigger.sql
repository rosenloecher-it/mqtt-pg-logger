
-- DROP TRIGGER IF EXISTS journal_json_trigger ON journal;

CREATE TRIGGER journal_json_trigger
  BEFORE INSERT
  ON journal
  FOR EACH ROW
  EXECUTE PROCEDURE journal_text_to_json();

-- INSERT INTO JOURNAL (message_id, topic, text, qos, retain) values (1, 'topic', '{"a": "json"}', 1, 0);
-- SELECT * FROM journal;
