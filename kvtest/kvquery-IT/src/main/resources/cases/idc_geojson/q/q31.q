#geo_intersect: Returns false if any operand returns more then 1 items.

select id,
       p.info.point,
       geo_intersect(p.info.values(),
                   {"type": "multipoint", "coordinates": [[ 79.85287606716155, 6.934201673601963],[79.85367000102997,6.934451956982983]]}
) as intersect
from points p
order by id
