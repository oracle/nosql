select sum(f.mixed.x) as sum, count(*) as cnt, f.mixed.a, avg(f.mixed.x) as avg
from Foo f
group by f.mixed.a
order by 2 * avg(f.mixed.x) + 1
