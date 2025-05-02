#expression returns second using Extract Function for json array 
SELECT id,extract(second from cast(t.json.at[0].values($key="k1") as timestamp)) as ats0,extract(second from cast(t.json.at[2] as timestamp)) as ats2 from Extract t
