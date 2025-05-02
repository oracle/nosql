#left operand of the geo_near function is not a json object. arg type = Integer
select id
from points p
where geo_near(1,2,3)
