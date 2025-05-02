select id 
from foo2 f 
where f.json[] >any cast("2022-03-18" as timestamp)
