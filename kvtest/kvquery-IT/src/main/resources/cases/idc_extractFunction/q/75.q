#expression returns null using Extract Function for a Empty value. 
SELECT id,extract(microsecond from t.json.ts3) FROM Extract t