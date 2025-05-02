#
# places within 10km from chania city street
#
declare $dist integer; // 10000
        $city string; // platanias
select
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
where p.info.city in ($city, "gerani", "kandanos") and
      geo_near(p.info.geom, 
               { "type" : "linestring",
                  "coordinates" : 
                  [ 
                    [24.0262, 35.5043],
                    [24.0202, 35.5122]
                  ]
               },
               $dist)
