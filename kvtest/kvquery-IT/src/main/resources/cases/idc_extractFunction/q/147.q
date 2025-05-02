#expression returns millisecond using millisecond()function for json map using cast
SELECT id,millisecond(cast(t.json.k.values($key="k4") as timestamp ))as jats2 FROM Extract t
