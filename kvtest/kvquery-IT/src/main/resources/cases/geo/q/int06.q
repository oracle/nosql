#
# box in iraklion county
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [24.9609375, 35.15625],
                                          [25.3125,    35.15625],
                                          [25.3125,    35.33203125],
                                          [24.9609375, 35.33203125],
                                          [24.9609375, 35.15625]
                                      ] ]
                    } )
