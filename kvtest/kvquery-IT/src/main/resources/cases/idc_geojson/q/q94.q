#The left operand of the geo_near function is not a valid geometry.
select id
from points p
where geo_near({},{},{})
