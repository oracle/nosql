#geo_is_geometry(any*): operand returns 0 item

select id,
p.info.point,
geo_is_geometry(p.info.test)
from points p
