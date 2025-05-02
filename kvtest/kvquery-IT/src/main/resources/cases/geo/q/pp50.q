#
# All hotels in chania county
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where p.info.kind = "hotel" and
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
