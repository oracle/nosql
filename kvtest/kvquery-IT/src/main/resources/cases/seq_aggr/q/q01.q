
select id, 
       seq_sum(f.info.expenses.values()) as sum,
       seq_avg(f.info.expenses.values()) as avg
from foo f
order by id
