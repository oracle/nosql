#expression returns nanosecond for Json Atomic Type using Cast. 2038 problem.
SELECT id,extract(nanosecond from cast(t.json.y38 as timestamp)) FROM Extract t 