# select all
select * from A.B b left outer join A.B.C c on b.ida1 = c.ida1 and
                        b.idb1 = c.idb1 and b.idb2 = c.idb2
