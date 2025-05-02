#expression returns null using month() timestamp Function for a NULL value. 
SELECT id,month(cast(t.json.k[].a as timestamp ))as jats2  FROM Extract t WHERE id=2
