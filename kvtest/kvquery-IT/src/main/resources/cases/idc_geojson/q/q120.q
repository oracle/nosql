#geo_is_geometry():operand returns in-valid GeoJson object. 
# Invalid String "cooordinates"

select id, geo_is_geometry({ "type" : "point", "cooordinates" : [ 77.58909702301025, 12.97269293084298 ] }) 
from geotypes g
where id =1

