# order by
select a.ida1, a2, a3, b.idb1, b.idb2, b3, b4
  from A.B b left outer join A a on b.ida1 = a.ida1
  order by b.ida1, b.idb1, b.idb2
