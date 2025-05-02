#expression returns hour for Json Atomic Type using Cast. 2038 problem.
SELECT id,extract(hour from cast(t.json.Y38 as timestamp)) FROM Extract t 