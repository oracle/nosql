#expression returns second using second()function for json array using cast
SELECT id,second(cast(t.json.at[2] as timestamp ))as jats2,second(cast(t.json.at[0].values() as timestamp))as jats0 FROM Extract t
