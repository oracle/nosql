#expression returns minute using Extract Function. If the result of the expression is NULL, extract aslo returns NULL.
SELECT id,extract(minute from cast(t.json.at[2] as timestamp)) FROM Extract t