# Test for regex_like function with expression having array slice step

select p.info.id.connections[0:4] as strongConnections,p.info.id.name 
from playerinfo p 
where regex_like(p.info.id.name,".*a.*","i")