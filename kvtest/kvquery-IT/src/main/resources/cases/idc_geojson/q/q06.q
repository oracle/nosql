# Distance from Delhi to London: 6723 km

select id,
       p.info.point,
       geo_distance(p.info.point,
                   { "type" : "point", "coordinates" : [ -0.12737274169921875, 51.50735350177636] }) as dist
from points p
where id = 9
