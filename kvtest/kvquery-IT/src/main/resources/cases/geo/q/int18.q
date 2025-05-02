#
# Chania county and chania port
#
select /* FORCE_PRIMARY_INDEX(points) */
       id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "MultiPolygon",
                      "coordinates" : 
                      [ 
                          [
                            [ [24.013335, 35.518654],
                              [24.016038, 35.516646],
                              [24.017927, 35.516332],
                              [24.018742, 35.517676],
                              [24.024471, 35.518114],
                              [24.023699, 35.520139],
                              [24.016296, 35.520191],
                              [24.013335, 35.518654]
                            ]
                          ],
                          [
                            [            
                              [23.48, 35.16],
                              [24.30, 35.16],
                              [24.30, 35.70],
                              [23.48, 35.70],
                              [23.48, 35.16]
                            ] 
                          ]
                      ]
                    } )
