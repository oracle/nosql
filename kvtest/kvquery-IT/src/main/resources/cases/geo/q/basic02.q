select id,
       p.info.point,
      geo_distance(p.info.point,
                   { "type" : "point", "coordinates" : [ 24.0175, 35.5156 ] }) as dist
from points p
where id < 2
