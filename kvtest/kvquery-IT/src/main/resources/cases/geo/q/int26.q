#
# England, crossing prime meridian
#
select id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [-1.53, 51.66],
                                          [ 0.61, 51.66],
                                          [ 0.61, 52.46],
                                          [-1.53, 52.46],
                                          [-1.53, 51.66]
                                      ] ]
                    } )
