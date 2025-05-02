select id, 
       seq_max(f.info.expenses.values()) as max,
       seq_count(f.info.expenses.values()) as cnt
from foo f
order by id
