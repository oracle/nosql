#geo_distance: Returns error if any operand returns an item that is not a valid GeoJson object.

select id,
       p.info.point,
       geo_distance(null,{"type": "multipoint", "coordinates": [[ 79.85287606716155, 6.934201673601963],[79.85367000102997,6.934451956982983]]}
) as dist
from points p
