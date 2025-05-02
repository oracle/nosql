select
    id,
    str1,
    index_of(str1, 'a') as a1,
    index_of(str1, 'c') as c1,
    index_of(str1, 'a', 2) as a2,
    index_of(str1, 'abc') as abc1,
    index_of(str1, 'abc', -4) as abc_4,
    index_of(str1, 'abc', 3) as abc_3,
    index_of(str1, 'abc', 30) as abc_30
from
    stringsTable ORDER BY id