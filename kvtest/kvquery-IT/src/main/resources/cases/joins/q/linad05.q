# index_storage_size on descendant table

select $b.ida as b_ida, $b.idb as b_idb,
       $a.ida as a_ida, $a.a1, 
       $d.ida as d_ida, $d.idb as d_idb, $d.idc as d_idc, $d.idd as d_idd,
       50 <= index_storage_size($d, "d_idx_d2_idb_c3") and 
       index_storage_size($d, "d_idx_d2_idb_c3") < 65 as size_d_idx_d2_idb_c3 
from nested tables(A.B $b ancestors(A $a) descendants(A.B.C.D $d))
where $b.ida != 40
order by $b.ida, $b.idb
