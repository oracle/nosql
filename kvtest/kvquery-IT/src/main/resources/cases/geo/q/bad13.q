select geo_distance({ "type" : "point", "coordinates" : [ 2.75, 48.44 ]}, p.info)
from points p
where id < 10

