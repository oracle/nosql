#expression returns isoweek using Extract Function. If the result of the expression is NULL, extract aslo returns NULL.
SELECT id,extract(isoweek from cast(t.ts5 as timestamp)) FROM Extract t