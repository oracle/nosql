# geo_near is in where clause nested under NOT operator

select id
from points p
where not
      geo_near(p.info.point, { "type" : "multipoint", "coordinates" : [ [77.5909423828125,12.983147716796578],[77.55575180053711,13.012000642911662] ] },
                          300000.098)