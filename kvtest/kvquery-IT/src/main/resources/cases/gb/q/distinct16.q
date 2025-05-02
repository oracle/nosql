select distinct avg(f.xact.item.price) as avg
from Foo f
group by f.xact.year, f.xact.prodcat

#select f.xact.year, f.xact.prodcat, avg(f.xact.item.price) as avg
#from Foo f
#group by f.xact.year, f.xact.prodcat
