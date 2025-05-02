declare $badPolygon json;
select *
from points p
where geo_intersect(p.info.point, $badPolygon) 

