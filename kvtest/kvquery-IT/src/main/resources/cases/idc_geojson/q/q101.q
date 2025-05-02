#operand3 is int.
select id
from points p
where geo_near(p.info.point,p.info.point,23232323) 
