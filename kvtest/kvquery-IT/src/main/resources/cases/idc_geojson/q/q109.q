#geo_is_geometry(any*): operand returns > 1 items

select id,
p.info.point,
geo_is_geometry(p.info.values())
from points p
