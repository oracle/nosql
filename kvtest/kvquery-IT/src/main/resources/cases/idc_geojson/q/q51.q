#geo_within_distance: Returns error if any of the first two operands returns an item that is not a valid GeoJson object

select id,
       p.info.point,
       geo_within_distance(null,{"type": "point", "coordinates": [ 79.85287606716155, 6.934201673601963]},200000
)
from points p
