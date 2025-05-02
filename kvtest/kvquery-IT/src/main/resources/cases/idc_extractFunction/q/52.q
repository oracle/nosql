#expression returns nanosecond using Extract Function for json array 
SELECT id,extract(nanosecond from cast(t.json.at[0].values($key="k1") as timestamp)) as ats0,extract(nanosecond from cast(t.json.at[2] as timestamp)) as ats2 from Extract t
