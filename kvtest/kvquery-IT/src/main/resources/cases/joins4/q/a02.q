# order by
select a.ida1, a2, a3, b.idb1, b.idb2, b3, b4 
  from nested tables (A.B b ancestors (A a)) 
  order by b.ida1, b.idb1, b.idb2
