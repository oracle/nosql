# aggregate function + group by
select b.ida1, min(a2) as min_of_a2, max(a2) as max_of_a2, sum(c.idc2) as sum_of_idc2 from nested tables (A.B b ancestors (A a) descendants (A.B.C c)) group by b.ida1
