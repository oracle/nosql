#operand3 is non numeric.
select geo_within_distance(p.info.point,p.info.point,"test") from points p where id = 1
