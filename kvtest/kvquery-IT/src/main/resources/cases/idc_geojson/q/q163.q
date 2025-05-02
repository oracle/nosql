#
# boolean geo_within_distance(any*, any*, double)
# Returns NULL if any operand returns NULL.
#
select geo_within_distance(t.info.point, t.info1, 121212) from testsqlnull t