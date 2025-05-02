#expression returns null using Extract Function for a Empty value. 
SELECT id,extract(millisecond from t.json.ts3) FROM Extract t