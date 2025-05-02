select id
from points p
where p.info.kind = "hotel" or 
      geo_near(p.info.point, 
               { "type" : "multipoint",
                 "coordinates" : [ [24.0175, 35.5156], [23.6793, 35.2203 ] ] },
                  5000)
