#geo_distance: Raises an error if it can be detected at compile time that an operand will not return a single valid GeoJson object.

select id,
       p.info.point,
       geo_distance(p.info.point,{"test":"testval"}) as distance
from points p
