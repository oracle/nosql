select f.xact.acctno,
       f.xact.year,
       count(*) as count,
       avg(f.xact.item.qty * f.xact.item.price) as sales
from Foo f
group by f.xact.acctno, f.xact.year
