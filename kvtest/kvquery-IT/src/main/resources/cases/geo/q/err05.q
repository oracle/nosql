select id,
       geo_near(p.info.point, 
               { "type" : "point",
                 "coordinates" : [ 24.6793, 35.2203 ] },
                  5000)
from points p
