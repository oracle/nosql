# geo_near()
# Verify (implicit) order by the distance between point and multipoint
#
select id, 
       cast(geo_distance(p.info.point,
                         { 
                           "type" : "multipoint", 
                           "coordinates" : [ 
                             [77.5909423828125,12.983147716796578],
                             [77.55575180053711,13.012000642911662] 
                           ] 
                         }) * 1000000
            as long ) as dist
from points p
where geo_near(p.info.point, 
               { 
                 "type" : "multipoint", 
                 "coordinates" : [ 
                   [77.5909423828125,12.983147716796578],
                   [77.55575180053711,13.012000642911662] 
                 ] 
               },
               300000.098)