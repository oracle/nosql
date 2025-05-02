#
# Distance between Bengaluru and chennai within 200km. 
# operand1 is Point and operand2 is Point, 1st geometry is within distance specified by operand3 from 2nd geometry
# Should return true.
#
select geo_within_distance(p.info.point,
                          { "type" : "point", "coordinates" : [77.5909423828125,12.983147716796578 ] },
                          300000.098)
from points p
where id =18