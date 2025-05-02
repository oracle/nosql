select b.xact.state, count(*) as count
from bar b
group by b.xact.state
order by b.xact.state desc nulls last
