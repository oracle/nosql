#geo_inside: return false as p.info.point.kind is not a GeoJson object

select id,
       p.info.point,
       geo_inside(p.info.point.kind,{"type": "multipoint", "coordinates": [[ 79.85287606716155, 6.934201673601963],[79.85367000102997,6.934451956982983]]}
)
from points p
where id = 17
