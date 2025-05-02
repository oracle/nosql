#
# operand1 is Point and operand2 is MultiPoint, 1st geometry is within distance specified by operand3 from 2nd geometry
# Should return true.
#
select geo_within_distance(p.info.point,
                          { "type" : "multipoint", "coordinates" : [ [77.5909423828125,12.983147716796578],[77.55575180053711,13.012000642911662] ] },
                          300000.098)
from points p
where id =18