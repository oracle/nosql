select geo_distance(t.geojson.features[0].geometry,t.geojson.features[1].geometry) 
from geosrs t 
where pk=2