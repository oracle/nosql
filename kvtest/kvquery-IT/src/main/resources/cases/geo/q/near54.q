#
# places within 10km from chania city street
#
declare $arr2 array(string); // [ "platanias", "gerani", "kandanos" ]
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
where p.info.city in $arr2[] and
      geo_near(p.info.geom, 
               { "type" : "linestring",
                  "coordinates" : 
                  [ 
                    [24.0262, 35.5043],
                    [24.0202, 35.5122]
                  ]
               },
               10000)
