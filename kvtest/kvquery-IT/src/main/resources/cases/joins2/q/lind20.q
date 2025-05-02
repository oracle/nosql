#
# TODO: make this work
#
#select count(*) as cnt
select *
from nested tables(A a descendants(A.B b, A.B.C c, A.B.C.D d))
#order by a.ida
#limit 10
