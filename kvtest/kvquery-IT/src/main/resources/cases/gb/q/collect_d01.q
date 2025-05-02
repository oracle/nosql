select f.xact.acctno, array_collect(distinct f.xact.prodcat), count(*)
from Foo f
group by f.xact.acctno
