#expression returns year using Extract Function with Exist, not Exists Operator on Json Map.
SELECT id, extract(year from cast(t.json.k.k4 as timestamp)) as jmap1,extract(year from cast(t.json.k.k4 as timestamp)) as jmap2 FROM Extract t  WHERE exists extract(year from cast(t.ts4 as timestamp))
