# geo_inside(): Raises an error if it can be detected at compile time that an operand will not return a single valid GeoJson object
select geo_inside(snull,jnull) from points p
