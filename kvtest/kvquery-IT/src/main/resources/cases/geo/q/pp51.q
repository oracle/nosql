#
# All hospitals in west chania city 
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where p.info.kind = "hospital" and
      geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [24.00, 35.50],
                                          [24.02, 35.50],
                                          [24.02, 35.52],
                                          [24.00, 35.52],
                                          [24.00, 35.50]
                                      ] ]
                    } )
