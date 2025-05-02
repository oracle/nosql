#The right operand of the geo_distance function is not a valid geometry.
select geo_distance(null,{}) from points where id = 1
