select f.lng + 1,
       sum(f.age * f.id) + sum(f.dbl)
from ComplexType f where (f.id,f.age) IN ((0,10),(1,11)) and f.flt >any 4.5
group by f.lng
order by count(f.lng)
