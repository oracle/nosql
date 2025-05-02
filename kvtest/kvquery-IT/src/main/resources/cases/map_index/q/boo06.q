select id
from boo b
where b.expenses."[]" = 3 and b.expenses."values()" > 10
