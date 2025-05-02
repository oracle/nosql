#
# All hotels within 5km from chania or paleochora city center
#
select id, 
       geo_distance(p.info.point,
                    { "type" : "multipoint",
                      "coordinates" : [ [24.0175, 35.5156], [23.6793, 35.2203 ] ] })
from points p
where p.info.kind = "hotel" and 
      geo_near(p.info.point, 
               { "type" : "multipoint",
                 "coordinates" : [ [24.0175, 35.5156], [23.6793, 35.2203 ] ] },
                  5000)
