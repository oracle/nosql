select distinct f.xact.acctno, f.xact.year
from foo f
order by f.xact.acctno
limit 4
offset 3
