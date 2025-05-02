# Distance from Quito,Ecuador to Pakanbaru,Indonesia:19,968 km

select id,
       p.info.point,
       geo_distance(p.info.point,
                   { "type" : "point", "coordinates" : [ 101.45050048828125, 0.537807196435151] }) as dist
from points p
where id = 7
