#operand3 is non numeric.
select id
from points p
where geo_near(p.info.point,p.info.point,"test")
