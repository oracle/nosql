#expression returns second for Json Atomic Type using Cast. 2038 problem.
SELECT id,extract(second from cast(t.json.y38 as timestamp)) FROM Extract t 