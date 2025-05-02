# geo_near()
# operand1 is Point and operand2 is Point, 1st geometry is within distance specified by operand3 from 2nd geometry
#
select id, geo_distance(p.info.point,
                    { "type" : "point", "coordinates" : [77.5909423828125,12.983147716796578 ] }) as dist
from points p
where p.info.kind="port"
and geo_near(p.info.point,
                          { "type" : "point", "coordinates" : [77.5909423828125,12.983147716796578 ] },
                          300000.098)