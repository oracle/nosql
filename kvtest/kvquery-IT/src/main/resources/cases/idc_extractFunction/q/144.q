#expression returns hour using hour()function for json array using cast
SELECT id,hour(cast(t.json.at[2] as timestamp ))as jats2,hour(cast(t.json.at[0].values() as timestamp))as jats0 FROM Extract t
