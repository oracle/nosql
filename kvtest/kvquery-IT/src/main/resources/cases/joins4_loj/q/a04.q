# on clause + value comparison operators + where clause + is not null
select *
from A.B.D d left outer join A a on d.ida1 = a.ida1 and a.a2 >= 0
             left outer join A.B b on d.ida1 = b.ida1 and d.idb1 = b.idb1 and
                                      d.idb2 = b.idb2
where a.a3 is not null
