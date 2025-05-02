#operand3 is numeric and of number type.
select id
from points p
where geo_near(p.info.point,p.info.point,11111111111111111111111111111111111111111111111111111)
