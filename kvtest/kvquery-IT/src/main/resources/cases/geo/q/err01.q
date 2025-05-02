
select *
from points
where geo_intersect(point, { "type" : "polygon", [ })
