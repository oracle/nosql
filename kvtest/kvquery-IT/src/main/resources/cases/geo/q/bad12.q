select id,
       geo_distance(p.info.point,
                    { "type" : "point", "coordinates" : [ -105.372036,-85 ] })
from points p
where id = 1009
 
