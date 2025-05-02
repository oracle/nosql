#The right operand of the geo_distance function is not a valid geometry.
select geo_within_distance(null,{},null) from points where id = 1
