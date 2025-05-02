#
# boolean geo_is_geometry(any*)
# Returns NULL if any operand returns NULL.
#
select geo_is_geometry(t.info1) from testsqlnull t