# Distance from bengalurur to delhi:1744 km

select id,
       p.info.point,
       geo_distance(p.info.point,
                   { "type" : "point", "coordinates" : [ 77.23116159439087, 28.61296965937075] }) as dist
from points p
where id = 8
