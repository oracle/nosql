#expression returns week using Extract Function. If the result of the expression is NULL, extract aslo returns NULL.
SELECT id,extract(week from cast(t.ts5 as timestamp)) FROM Extract t