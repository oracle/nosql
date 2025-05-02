#
# Single point in Chania
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "point",
                      "coordinates" : [ 24.016, 35.506 ]
                    } )
