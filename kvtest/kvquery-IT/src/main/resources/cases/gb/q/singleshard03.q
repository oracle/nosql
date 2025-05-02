select id1, f.xact.year
from Bar f
where id1 = 0 and f.xact.items[].qty >any 2
order by f.xact.year
limit 6
offset 2
