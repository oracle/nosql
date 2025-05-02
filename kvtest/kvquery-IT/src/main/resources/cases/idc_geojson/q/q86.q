# geo_near is not in where clause

select id, geo_distance(p.info.point,
                          { "type" : "multipoint", "coordinates" : [ [77.5909423828125,12.983147716796578],[77.55575180053711,13.012000642911662] ] }) as dist,
geo_near(p.info.point, { "type" : "multipoint", "coordinates" : [ [77.5909423828125,12.983147716796578],[77.55575180053711,13.012000642911662] ] },
                          300000.098)
from points p