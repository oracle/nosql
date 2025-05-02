#
# Short Vertical line in antarctica
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "LineString",
                      "coordinates" : [ [-100, -89.6], [-100, -89.5] ]
                    } )
