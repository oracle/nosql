# aggregate function + JSON path expression + map field step expression
select b.ida1, min(b.b4.comment) as min_of_b4, max(b.b4.comment) as max_of_b4, sum(c.c3.ckey1) as sum_of_c3 from nested tables (A.B b ancestors (A a) descendants (A.B.C c)) group by b.ida1
