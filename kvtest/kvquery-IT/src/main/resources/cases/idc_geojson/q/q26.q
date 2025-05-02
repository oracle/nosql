#The left operand of the geo_intersect function is not a valid geometry.
select geo_intersect({},{}) from points where id = 1
