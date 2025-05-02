
select seq_sum(f.info.expenses.values($value < 100)) as sum
from foo f
