select distinct f.info.bar1, f.info.bar2, count(f.info.bar1)
from fooNew f
where f.info.bar1<=7
group by f.info.bar1, f.info.bar2
order by count(f.info.bar1)
