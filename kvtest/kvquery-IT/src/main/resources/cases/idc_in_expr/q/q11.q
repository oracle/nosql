select id
from ComplexType f
where (f.age, f.firstName) in ((seq_concat(f.age),seq_concat(f.firstName)))
