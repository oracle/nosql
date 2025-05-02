select distinct f.xact.acctno
from Bar f
where f.xact.year = 2000 and f.xact.items[].qty >any 2
order by f.xact.acctno
