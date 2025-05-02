#round with index
select id,trunc(fc,2) as fc, round(fc,2)
from math_test
where round(fc,2)>0 order by id
