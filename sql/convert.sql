CREATE OR REPLACE FUNCTION journal_text_to_json()
  RETURNS TRIGGER
  LANGUAGE PLPGSQL
  AS
$$
BEGIN
	IF NEW.data IS NULL AND NEW.text IS NOT NULL THEN
        BEGIN
            NEW.data = NEW.text::JSON;
        EXCEPTION WHEN OTHERS THEN
            NEW.data = NULL;
        END;
	END IF;

	RETURN NEW;
END;
$$
