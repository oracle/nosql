# on clause + value comparison operators + where clause + is not null
select *
from nested tables (A.B.D d ancestors (A.B b, A a on a.a2 >= 0))
where a.a3 is not null
