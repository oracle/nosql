#expression returns nanosecond using Extract Function. If the result of the expression is NULL, extract aslo returns NULL.
SELECT id,extract(nanosecond from cast(t.ts4 as timestamp)) FROM Extract t