# geo_near()
# Verify (implicit) order by the distance between two points.

select id, 
       cast(geo_distance(p.info.point,
                         { 
                           "type" : "point", 
                           "coordinates" : [77.5909423828125,12.983147716796578 ] 
                         }) * 1000000 
            as long) as dist
from points p
where geo_near(p.info.point,
               { 
                 "type" : "point", 
                 "coordinates" : [77.5909423828125,12.983147716796578 ] 
               },
               300000.098)