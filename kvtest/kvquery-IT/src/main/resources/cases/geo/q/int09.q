#
# Long Horizontal line in antarctica
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "LineString",
                      "coordinates" : [ [-105.0, -85.0], [-80, -85.0] ]
                    } )
