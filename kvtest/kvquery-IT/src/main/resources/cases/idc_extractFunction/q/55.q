#expression returns year using Extract Function for json map 
SELECT id,extract(year from t.json.k.values($key="k4")) as mts4 FROM Extract t
