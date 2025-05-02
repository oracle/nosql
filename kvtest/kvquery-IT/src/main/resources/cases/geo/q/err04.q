select id
from points p
where geo_near(p.info.point, 
               { "type" : "point",
                 "coordinates" : [ 24.6793, 35.2203 ] },
                  5000)
      and
      geo_near(p.info.point, 
               { "type" : "multipoint",
                 "coordinates" : [ [24.0175, 35.5156], [23.6793, 35.2203 ] ] },
                  5000)
