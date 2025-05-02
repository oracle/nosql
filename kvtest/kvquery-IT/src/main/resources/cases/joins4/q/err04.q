# Table A.B is not an ancestor of target table A.B
select * from nested tables (A.B b1 ancestors (A.B b2))
