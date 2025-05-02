select id
from ComplexType f
where ((f.age) in (1, 2, (seq_concat(f.age)), null)) and ((cast (7 as Long))  in f.children.values().age)

