select b.xact.state, array_collect(b.xact.city) as cities
from bar b
group by b.xact.state
order by b.xact.state desc
