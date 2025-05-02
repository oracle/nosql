# Test for regex_like function with expression having map field step

select id1, name,p.info.id.expenses.books 
from playerinfo p 
where regex_like(p.name,"s.*","i")