#operand3 is long.
select id
from points p
where geo_near(p.info.point,p.info.point,9223372036854775807)
