#
# places within 5km from chania north highway
#
select /* FORCE_PRIMARY_INDEX(points) */
       id,
       p.info.geom,
       geo_distance(p.info.geom,
                    { "type" : "linestring",
                      "coordinates" :
                      [ 
                        [24.0254, 35.4849],
                        [23.9770, 35.5070],
                        [23.7676, 35.5347],
                        [23.6536, 35.4900]
                      ]
                    })
from polygons p
where geo_near(p.info.geom, 
               { "type" : "linestring",
                  "coordinates" : 
                  [ 
                    [24.0254, 35.4849],
                    [23.9770, 35.5070],
                    [23.7676, 35.5347],
                    [23.6536, 35.4900]
                  ]
               },
               5000)
