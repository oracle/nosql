select min(f.xact.item.qty * f.xact.item.price) as min, f.xact.acctno, f.xact.year
from Foo f
group by f.xact.acctno, f.xact.year
