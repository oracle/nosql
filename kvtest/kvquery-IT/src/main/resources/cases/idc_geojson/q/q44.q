#
# operand1 is Point and operand2 is MultiPoint, 1st geometry is not within distance specified by operand3 from 2nd geometry
# Should return false.
#
select geo_within_distance(p.info.point,
                          { "type" : "multipoint", "coordinates" : [ [77.5909423828125,12.983147716796578],[77.55575180053711,13.012000642911662] ] },
                          200000.098)
from points p
where id =18