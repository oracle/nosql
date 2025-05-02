#expression returns day using Extract Function for json map 
SELECT id,extract(day from t.json.k.values($key="k4")) as mts4 FROM Extract t
