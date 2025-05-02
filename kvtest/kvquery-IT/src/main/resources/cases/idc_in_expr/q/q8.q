select id
from ComplexType f
where (f.age) in (seq_concat(f.age))
