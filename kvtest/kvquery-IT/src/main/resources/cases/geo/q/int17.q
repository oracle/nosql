#
# Huge box in the eastern hemisphere
#
select id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [0,   0],
                                          [80,  0],
                                          [80, 60],
                                          [0,  60],
                                          [0,   0]
                                      ] ]
                    } )
