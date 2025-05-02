# target table alias conflicts with ancestor table alias
select * from nested tables (A.B b ancestors (A b))
