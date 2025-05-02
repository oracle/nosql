#geo_is_geometry(): Returns false if the operand returns zero or more than 1 items.
# Returns false if the input is not a single valid GeoJson object. In this case p.info.point.kind is jnull.

select id,
p.info.point,
geo_is_geometry(p.info.point.kind)
from points p
where id = 17
