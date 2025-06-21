-- Run via Porter or DuckDB
CREATE TABLE quotes (quote TEXT);
INSERT INTO quotes VALUES 
 ('SELECT * FROM life WHERE happy;'),
 ('DELETE FROM sadness WHERE reason IS NOT NULL;'),
 ('WITH joy AS (SELECT * FROM today) SELECT * FROM joy;'),
 ('SELECT COUNT(*) FROM good_days WHERE mood = ''awesome'';');
