#expression returns millisecond using Extract Function. If the result of the expression is NULL, extract aslo returns NULL.
SELECT id,extract(millisecond from cast(t.ts2 as timestamp)) FROM Extract t