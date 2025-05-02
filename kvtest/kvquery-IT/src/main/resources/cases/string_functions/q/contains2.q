select id,
    contains(str1, str2) as c,
    starts_with(str1, str2) as s,
    ends_with(str1, str2) as e
from stringsTable ORDER BY id