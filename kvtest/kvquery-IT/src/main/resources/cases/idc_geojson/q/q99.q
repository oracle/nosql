#operand3 is " ".
select id
from points p 
where geo_near(p.info.point,p.info.point," ")
