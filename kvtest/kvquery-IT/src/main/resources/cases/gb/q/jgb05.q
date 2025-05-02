select f.xact.acctno,
       f.xact.year,
       f.xact.prodcat,
       sum(f.xact.item.discount) as sum
from Foo f
group by f.xact.acctno, f.xact.year, f.xact.prodcat
