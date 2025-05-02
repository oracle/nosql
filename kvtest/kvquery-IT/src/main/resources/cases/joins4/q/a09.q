# secondary index idx_d_d23 on A.B.D(d2, d3)
select d.ida1, d.idd1, d2, d3, d2+d3 as sum, a2, a3 from nested tables (A.B.D d ancestors (A a)) where d2 > 0 and d3 > 5000

