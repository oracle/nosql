#
# double geo_distance(any*, any*)
# Returns NULL if any operand returns NULL.
#
select geo_distance(t.info.point, t.info1) from testsqlnull t