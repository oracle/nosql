#operand3 is jnull.
select id
from points p 
where geo_near(p.info.point,p.info.point,jnull)
