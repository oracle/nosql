select f.xact.storeid, count(*)
from Foo f
group by f.xact.storeid
order by f.xact.storeid
limit 3
offset 1
