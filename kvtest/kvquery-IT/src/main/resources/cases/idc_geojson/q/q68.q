#operand3 is long.
select geo_within_distance(p.info.point,p.info.point,9223372036854775807) from points p where id = 1
