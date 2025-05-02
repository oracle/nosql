# order by desc
select c.ida1, c.idb1, c.idb2, c.idc1, c.idc2, c3, a2, a3
  from A.B.C c left outer join A a on c.ida1 = a.ida1
  order by c.ida1 desc, c.idb1 desc, c.idb2 desc, c.idc1 desc
