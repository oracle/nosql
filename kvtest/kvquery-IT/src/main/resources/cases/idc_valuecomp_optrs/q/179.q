# Test Description: operand1 returns more than one item using != operator.

select id1, f.map.values() != f.rec.fmap
from foo f
order by id1