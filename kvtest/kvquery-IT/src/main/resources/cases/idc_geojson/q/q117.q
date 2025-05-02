#geo_is_geometry():operand returns valid GeoJson MultiPolygon object

select id, geo_is_geometry(g.info.geom) 
from geotypes g
where id=7

