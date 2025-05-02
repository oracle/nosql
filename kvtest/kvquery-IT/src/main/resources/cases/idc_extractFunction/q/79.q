#expression returns year using Extract Function. If the result of the expression is NULL, extract aslo returns NULL.
SELECT id,extract(year from cast(t.json.k.k4 as timestamp)) FROM Extract t