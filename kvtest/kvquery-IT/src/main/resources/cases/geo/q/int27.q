select id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [23.48, 35.16],
                                          [24.30, 35.16],
                                          [24.30, 35.70],
                                          [23.48, 35.70],
                                          [23.48, 35.16]
                                      ] ]
                    } )
       and
       p.info.point = {"coordinates":[23.549,35.2908],"type":"point"}

