#geo_within_distance: Returns false if any of the first two operands returns more 0 items.

select id,
       p.info.point,
       geo_within_distance(p.info.test,
                   {"type": "multipoint", "coordinates": [[ 79.85287606716155, 6.934201673601963],[79.85367000102997,6.934451956982983]]},200000
)
from points p
order by id
