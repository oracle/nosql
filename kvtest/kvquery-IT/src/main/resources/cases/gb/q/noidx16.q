select case
       when f.mixed.b is of type (number) then cast(f.mixed.b as double)
       else f.mixed.b
       end as b,
       sum(f.mixed.x) as sum, count(*) as cnt
from Foo f
group by f.mixed.b
