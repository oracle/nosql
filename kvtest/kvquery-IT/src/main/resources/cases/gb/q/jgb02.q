select f.xact.acctno, sum(f.xact.item.qty * f.xact.item.price), count(*)
from Foo f
where exists f.xact.acctno
group by f.xact.acctno
