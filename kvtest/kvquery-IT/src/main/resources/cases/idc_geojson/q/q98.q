#operand3 is null.
select id
from points p
where geo_near(p.info.point,p.info.point,null)
