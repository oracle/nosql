select f.xact.acctno, sum(f.xact.item.qty * f.xact.item.price), count(*)
from Foo f
group by f.xact.acctno
order by f.xact.acctno
