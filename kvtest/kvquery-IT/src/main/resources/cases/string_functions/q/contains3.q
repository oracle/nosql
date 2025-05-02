# null, which is a JSON null, will be converted by the implicit cast to the string "null"
select id,
    contains(null, "bc") as c,
    starts_with(null, "bc") as s,
    ends_with(null, "bc") as e
from stringsTable ORDER BY id