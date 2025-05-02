#geo_is_geometry():operand returns in-valid GeoJson object. 
# extra  "["

select geo_is_geometry({ "type" : "point", "coordinates" : [[ 77.58909702301025, 12.97269293084298 ] }) 
from geotypes g
where id =1

