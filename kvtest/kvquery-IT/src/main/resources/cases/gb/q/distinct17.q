select distinct avg(f.xact.item.price) as avg
from Foo f
group by f.xact.year, f.xact.prodcat
order by f.xact.year
