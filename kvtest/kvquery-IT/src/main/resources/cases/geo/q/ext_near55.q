#
# places within 10km from chania city street
#
declare $arr5 array(string); // [ "platanias", "gerani", "chania" ]
        $arr3 array(string); // [ "park", "village" ]
        $dist integer; // 10000
select /* FORCE_PRIMARY_INDEX(points) */
       id,
       p.info.geom,
       geo_distance(p.info.geom,
                    { "type" : "linestring",
                      "coordinates" :
                      [ 
                        [24.0262, 35.5043],
                        [24.0202, 35.5122]
                      ]
                    })
from polygons p
where p.info.kind in $arr3[] and
      p.info.city in $arr5[] and
      geo_near(p.info.geom, 
               { "type" : "linestring",
                  "coordinates" : 
                  [ 
                    [24.0262, 35.5043],
                    [24.0202, 35.5122]
                  ]
               },
               $dist)
