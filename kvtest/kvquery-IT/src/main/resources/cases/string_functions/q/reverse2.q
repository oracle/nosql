select id,
    reverse([][]) as r0,
    reverse(['abc'][]) as r1,
    reverse(['abc', 'def'][]) as r2
from
 stringsTable2 ORDER BY id