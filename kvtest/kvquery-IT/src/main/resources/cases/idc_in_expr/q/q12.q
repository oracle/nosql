select id , seq_sum(f.children.values().age) as sum from ComplexType as f where (CAST (f.age as Long)) IN (seq_sum(f.children.values().age)) order by id

