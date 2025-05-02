#geo_is_geometry():operand returns in-valid GeoJson object. 
# Invalid String "typee"

select id, geo_is_geometry({ "typee" : "point", "coordinates" : [ 77.58909702301025, 12.97269293084298 ] }) 
from geotypes g
where id =1

