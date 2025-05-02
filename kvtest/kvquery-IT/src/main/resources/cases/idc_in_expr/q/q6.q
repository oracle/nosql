select id
from ComplexType f
where (f.age, f.firstName) in ((seq_concat(f.age),"first0"))
