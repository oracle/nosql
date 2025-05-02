#
# Distance between Bengaluru and chennai within 200km. 
# operand1 is Point and operand2 is Point, 1st geometry is not within distance specified by operand3 from 2nd geometry
# Should return false.
#
select geo_within_distance(p.info.point,
                          { "type" : "point", "coordinates" : [77.5909423828125,12.983147716796578 ] },
                          200000.098)
from points p
where id =18