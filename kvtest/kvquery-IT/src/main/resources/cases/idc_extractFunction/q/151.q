#expression returns null using isoweek() timestamp Function for a Empty value. 
SELECT id,isoweek(cast(t.json.k.values($key="k4") as timestamp ))as jats2  FROM Extract t WHERE id=9
