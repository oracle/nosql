# Test Description: Both operand1 and operand2 return empty sequence using < operator.

select id1, foo.map.key10 < foo.rec.fmap.key10
from foo
order by id1