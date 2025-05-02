select f.lng + 1,
       (f.age * f.id) + sum(f.dbl)
from ComplexType f
group by f.lng
order by count(f.lng)
