select f.record.int + 1 as c1,
        2*(f.record.int - f.record.long) + 1 + f.record.int as c2,
        f.record.int + f.record.int + 1 as c3,
        count(*) as cnt
from Foo f
group by f.record.int - f.record.long, f.record.int
