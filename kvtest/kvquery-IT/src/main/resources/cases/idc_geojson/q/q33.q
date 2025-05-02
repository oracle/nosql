#geo_intersect: The right operand of the geo_inside function is not a valid geometry.

select p.info.point,
       geo_intersect(p.info.point,{"type": "multipoint"}) as intersect
from points p
where id = 18
