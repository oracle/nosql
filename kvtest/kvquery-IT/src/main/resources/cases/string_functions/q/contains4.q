# null, which is a JSON null, will be converted by the implicit cast to the string "null"
select id,
    contains(str1, null) as c,
    starts_with(str1, null) as s,
    ends_with(str1, null) as e
from stringsTable ORDER BY id