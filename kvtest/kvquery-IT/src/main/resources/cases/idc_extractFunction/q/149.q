#expression returns null using nanosecond() timestamp Function for a Empty value. 
SELECT id,nanosecond(cast(t.json.k.values($key="k4") as timestamp ))as jats2  FROM Extract t WHERE id=4
