select distinct count(*) as cnt
from Bar f
where id1 = 0 and f.xact.year = 2000 and f.xact.items[].qty >any 3
group by f.xact.acctno
