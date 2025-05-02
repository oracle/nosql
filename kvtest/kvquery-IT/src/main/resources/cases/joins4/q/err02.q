# target table alias conflicts with descendant table alias
select * from nested tables (A.B b descendants (A.B.C b))
