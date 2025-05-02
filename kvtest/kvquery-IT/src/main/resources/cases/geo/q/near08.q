#
# Short Horizontal line in antarctica (1925 m)
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_near(p.info.point, 
               { "type" : "LineString",
                 "coordinates" : [ [-105.372036, -85.0], [-105.174282, -85.0] ]
               },
               1300)
