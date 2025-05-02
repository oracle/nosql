#expression returns null using year() timestamp Function for a NULL value. 
SELECT id,year(cast(t.json.k as timestamp ))as jats2  FROM Extract t WHERE id=2
