# Raises an error if it can be detected at compile time that an operand will not return a single valid GeoJson object.

select geo_is_geometry(jnull) from points p
