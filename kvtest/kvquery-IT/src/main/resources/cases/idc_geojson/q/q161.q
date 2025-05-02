#
# geo_intersect(any*, any*)
# Returns NULL if any operand returns NULL.
#
select geo_intersect(t.info.point, t.info1) from testsqlnull t