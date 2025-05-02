#expression returns second using Extract Function for json map 
SELECT id,extract(second from cast(t.json.k.values($key="k4") as timestamp)) as mts4 FROM Extract t
