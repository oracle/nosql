select id,
       p.info.point,
       geo_distance(p.info.point,
                   { "type" : "point", "coordinates" : [ 77.5810718536377, 13.019359692750129 ] }) as dist_fromTrainingCommand
from points p
order by id