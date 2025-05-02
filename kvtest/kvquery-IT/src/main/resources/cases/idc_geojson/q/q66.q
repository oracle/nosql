#operand3 is jnull.
select geo_within_distance(p.info.point,p.info.point,jnull) from points p where id = 1
