#
# All hotels and hospitals in chania county and chania city
#
declare $arr1 array(string); // [ "hotel", "hospital" ]
select id, p.info.point
from points p
where p.info.kind in $arr1[] and
      p.info.city = "chania" and
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
