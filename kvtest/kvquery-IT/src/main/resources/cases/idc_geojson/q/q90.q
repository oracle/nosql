# geo_near is in where clause using AND operator

select id
from points p
where geo_near(p.info.point, { "type" : "multipoint", "coordinates" : [ [77.5909423828125,12.983147716796578],[77.55575180053711,13.012000642911662] ] },
                          300000.098)
and
      geo_near(p.info.point,
               { "type" : "multipoint",
                 "coordinates" : [ [24.0175, 35.5156], [23.6793, 35.2203 ] ] },
                  5000)