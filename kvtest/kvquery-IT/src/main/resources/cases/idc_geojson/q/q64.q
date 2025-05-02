#operand3 is null.
select geo_within_distance(p.info.point,p.info.point,null) from points p where id = 1
