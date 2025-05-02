select id, str1,
    contains(str1, "bc") as bc,
    starts_with(str1, "ab") as ab,
    ends_with(str1, "c") as c
from stringsTable ORDER BY id