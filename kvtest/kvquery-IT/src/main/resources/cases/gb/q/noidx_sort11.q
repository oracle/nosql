select f.xact.month, f.record.int, sum(f.record.long) as cnt
from Foo f
group by f.xact.month, f.record.int
order by f.record.int, f.xact.month
