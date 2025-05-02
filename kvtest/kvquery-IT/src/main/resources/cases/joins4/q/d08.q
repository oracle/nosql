# ON clause + arithmetic expressions + json index A.B(b4.comment as string)
select b.ida1, b.idb1, b.idb2, b.b4, d.idd1, d2, d3 from nested tables (A.B b descendants (A.B.D d on d2 + b.idb2 > 100)) where b.b4.comment='****'
