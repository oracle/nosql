#
# All points in chania county and east chania county
#
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
      geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [24.00, 35.19],
                                          [24.30, 35.19],
                                          [24.30, 35.50],
                                          [24.00, 35.50],
                                          [24.00, 35.19]
                                      ] ]
                    } )
