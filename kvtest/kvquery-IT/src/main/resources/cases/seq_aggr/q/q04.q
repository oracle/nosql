select id, 
       seq_min(f.info.expenses.values()) as min,
       f.info.expenses.keys($value = seq_min(f.info.expenses.values())) as min_cat
from foo f
order by id
