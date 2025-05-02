#expression returns month using Extract Function for json map 
SELECT id,extract(month from cast(t.json.k.values($key="k4") as timestamp)) as mts4 FROM Extract t
