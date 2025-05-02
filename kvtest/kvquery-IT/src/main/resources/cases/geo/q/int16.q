#
# Diagonal line in Chania county
#
select id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "LineString",
                      "coordinates" : [ [ 23.5943, 35.2481], [24.4564, 35.4550] ]
                    } )
