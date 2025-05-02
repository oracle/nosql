select f.xact.state, count(*) as cnt
from Foo f
where id1 = 0
group by f.xact.state
