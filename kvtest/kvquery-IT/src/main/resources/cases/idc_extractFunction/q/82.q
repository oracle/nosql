#expression returns day using Extract Function. If the result of the expression is NULL, extract aslo returns NULL.
SELECT id,extract(day from cast(t.json.at as timestamp)) FROM Extract t where id=2