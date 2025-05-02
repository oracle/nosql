#expression returns year,month,day and has logical operator AND using timestamp function for Atomic type and json map using cast
SELECT id,year(t.ts0) as ts0,month(cast(t.json.k.keys($key="k4") as timestamp ))as jats2,day(t.ts1)  FROM Extract t WHERE id=2 AND id=10 AND exists t.json
