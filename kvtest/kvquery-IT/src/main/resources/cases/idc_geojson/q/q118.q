#geo_is_geometry():operand returns in-valid GeoJson object

select id, geo_is_geometry(g.info.geom) 
from geotypes g

