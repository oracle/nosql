select f.xact.acctno, array_collect(f.xact.prodcat), count(*)
from Foo f
group by f.xact.acctno
