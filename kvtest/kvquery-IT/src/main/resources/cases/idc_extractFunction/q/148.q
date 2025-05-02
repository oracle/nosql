#expression returns microsecond using microsecond()function for json map using cast
SELECT id,microsecond(cast(t.json.k.values($key="k4") as timestamp ))as jats2 FROM Extract t
