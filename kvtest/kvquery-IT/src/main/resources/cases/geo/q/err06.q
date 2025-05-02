select id
from points p
where geo_near(p.info.point, 
               { "type" : "point",
                 "coordinates" : [ 24.6793, 35.2203 ] })
