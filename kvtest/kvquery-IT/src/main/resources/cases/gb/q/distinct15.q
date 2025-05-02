select distinct count(*) as count
from Foo f
group by f.xact.acctno, f.xact.year
order by avg(f.xact.item.price2)
