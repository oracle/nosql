
select id, p.info.point
from points p
where p.info.point = { "type" : "point", "coordinates" : [24.016, 35.506] }
      and
      geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [23.48, 35.16],
                                          [24.30, 35.16],
                                          [24.30, 35.70],
                                          [23.48, 35.70],
                                          [23.48, 35.16]
                                      ] ]
                    } )
