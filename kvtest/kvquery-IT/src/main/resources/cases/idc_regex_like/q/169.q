# Test for regex_like function with expression having map filter step

select id1, p.info.id.expenses.keys($value > 100) 
from playerinfo p 
where regex_like(p.name,"s.*","i")