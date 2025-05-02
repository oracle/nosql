#
# Horizontal line across crete
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "LineString",
                      "coordinates" : [ [23.5142, 35.3100], [26.3679, 35.3100] ]
                    } )
