
select f.xact.acctno, count(*) as count
from Foo f
group by f.xact.acctno, f.xact.year

