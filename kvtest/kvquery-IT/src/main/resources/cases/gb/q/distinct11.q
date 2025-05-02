select distinct f.xact.year
from Bar f
where f.xact.items[].qty >any 2
order by f.xact.year
