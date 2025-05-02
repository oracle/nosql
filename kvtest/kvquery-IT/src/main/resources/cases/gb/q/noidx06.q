select f.mixed.a,
       count(*) as cnt,
       min(f.mixed.x) as min,
       max(f.mixed.x) as max
from Foo f
group by f.mixed.a
