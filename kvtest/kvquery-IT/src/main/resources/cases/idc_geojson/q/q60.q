#The left operand of the geo_distance function is not a valid geometry.
select geo_distance({},{},{}) from points where id = 1
