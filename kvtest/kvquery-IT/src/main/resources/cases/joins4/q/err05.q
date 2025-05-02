# Table A.B is not a descendant of target table A.B
select * from nested tables (A.B b1 descendants (A.B b2))
