#expression returns microsecond for Json Atomic Type using Cast. 2038 problem.
SELECT id,extract(microsecond from cast(t.json.y38 as timestamp)) FROM Extract t 