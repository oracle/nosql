select f.xact.month, count(*)
from Foo f
where id1 = 0 and f.xact.year = 2000
group by f.xact.month

