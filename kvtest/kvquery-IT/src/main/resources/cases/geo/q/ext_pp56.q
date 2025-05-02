#
# All hotels and hospitals in chania county and chania city
#
declare $arr1 array(string); // [ "hotel", "hospital" ]
       $p1x double; // 23.48
select id, p.info.point
from points p
where p.info.kind in $arr1[] and
      p.info.city = "chania" and
      geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [$p1x, 35.16],
                                          [24.30, 35.16],
                                          [24.30, 35.70],
                                          [23.48, 35.70],
                                          [23.48, 35.16]
                                      ] ]
                    } )
