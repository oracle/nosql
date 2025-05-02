select *
from points
where geo_intersect(point, 
                    { "type" : "polygon",
                      "coordinates" : [ [-12, 3], ["-900", 3], [12, 3] ]
                    } )
