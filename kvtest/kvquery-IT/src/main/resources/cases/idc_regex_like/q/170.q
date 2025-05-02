# Test for regex_like function with expression having map filter step

select id1, 
seq_sum(p.info.id.expenses.values($key != "housing")) as sum, 
seq_max(p.info.id.expenses.values($key != "housing")) as max 
from playerinfo p 
where regex_like(p.name,"s.*","i")