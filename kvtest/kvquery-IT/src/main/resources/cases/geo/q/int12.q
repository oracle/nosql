#
# The Greenwich line
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "LineString",
                      "coordinates" : [ [0, -90], [0, 90] ]
                    } )
