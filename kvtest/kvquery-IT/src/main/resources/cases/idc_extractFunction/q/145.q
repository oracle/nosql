#expression returns minute using minute()function for json array using cast
SELECT id,minute(cast(t.json.at[2] as timestamp ))as jats2,minute(cast(t.json.at[0].values() as timestamp))as jats0 FROM Extract t
