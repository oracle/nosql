select f.xact.acctno,
       array_collect(distinct f.xact.prodcat) as prodcats,
       count(distinct f.xact.prodcat) as cnt
from Foo f
group by f.xact.acctno
order by count(distinct f.xact.prodcat), f.xact.acctno
