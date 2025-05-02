#
# Long Horizontal line in antarctica (241.45 km)
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, geo_distance(p.info.point, 
                        { "type" : "LineString",
                          "coordinates" : [ [-105.0, -85.0], [-80, -85.0] ]
                        }) as dist
from points p
where geo_near(p.info.point, 
               { "type" : "LineString",
                 "coordinates" : [ [-105.0, -85.0], [-80, -85.0] ]
               },
               20)
