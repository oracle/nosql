select geo_within_distance(t.geojson.features[0].geometry,t.geojson.features[1].geometry,1000) 
from geosrs t 
where pk=3