#
# boolean geo_inside(any*, any*)
# Returns NULL if any operand returns NULL.
#
select geo_inside(t.info.point, t.info1) from testsqlnull t