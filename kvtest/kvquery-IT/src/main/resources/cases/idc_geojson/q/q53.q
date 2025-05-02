#geo_within_distance: Returns false if any of the first two operands returns an item that is not a valid GeoJson object.

select id,
       p.info.point,
       geo_within_distance(p.info.point.kind,{"type": "multipoint", "coordinates": [[ 79.85287606716155, 6.934201673601963],[79.85367000102997,6.934451956982983]]},200000
)
from points p
where id = 17
