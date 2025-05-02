select f.xact.acctno, count(*)
from Foo f
where id1 = 0 and f.xact.year = 2000
group by f.xact.acctno
order by count(*)
