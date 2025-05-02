# Test Description: operand1 returns more than one item using >=s operator.

select id1, f.map.values() >= f.rec.fmap
from foo f
order by id1