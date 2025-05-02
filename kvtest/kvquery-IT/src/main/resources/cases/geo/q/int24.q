#
# Too large geometry
#
select id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [0,   0],
                                          [130, 0],
                                          [130, 60],
                                          [0,   60],
                                          [0,    0]
                                      ] ]
                    } )
