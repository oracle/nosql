#expression returns minute for Json Atomic Type using Cast. 2038 problem.
SELECT id,extract(minute from cast(t.json.y38 as timestamp)) FROM Extract t 