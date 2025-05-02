declare $p1x double; // 23.48
        $p1y double; // 35.16
select id, p.info.point
from points p
where geo_intersect(p.info.point, 
                    { "type" : "polygon",
                      "coordinates" : [ [
                                          [$p1x, $p1y],
                                          [24.30, 35.16],
                                          [24.30, 35.70],
                                          [23.48, 35.70],
                                          [$p1x, $p1y]
                                      ] ]
                    } )
