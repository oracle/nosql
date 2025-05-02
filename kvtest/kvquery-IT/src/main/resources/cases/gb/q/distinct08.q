select distinct f.xact.acctno, f.xact.year
from foo f
order by sum(f.xact.item.qty)
