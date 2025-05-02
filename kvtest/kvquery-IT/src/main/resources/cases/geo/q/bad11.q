select id, p.info.point 
from points p 
where geo_near(null,null,888)
