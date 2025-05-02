#expression returns month using Extract Function. If the result of the expression is NULL, extract aslo returns NULL.
SELECT id,extract(month from cast(t.json.k.k4 as timestamp)) FROM Extract t