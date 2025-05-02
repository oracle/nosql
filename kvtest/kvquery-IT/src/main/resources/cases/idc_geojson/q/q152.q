#geo_inside: The right operand of the geo_inside function is not a valid geometry.

select p.info.point,
       geo_inside(p.info.point,{"type": "multipoint"}) as inside
from points p
where id = 18
