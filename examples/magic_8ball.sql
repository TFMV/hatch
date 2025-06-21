-- Magic 8 Ball using SQL
-- Create table of possible answers
CREATE TABLE IF NOT EXISTS responses (answer TEXT);
INSERT INTO responses VALUES
  ('It is certain.'),
  ('Reply hazy, try again.'),
  ('Don't count on it.'),
  ('Signs point to yes.'),
  ('Very doubtful.');

-- Ask the Magic 8 Ball
SELECT answer FROM responses USING SAMPLE 1;
