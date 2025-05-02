# row_storage_size on target, ancestor, and descendant tables
# index_storage_size on anscestor table

select $b.ida as b_ida, $b.idb as b_idb,
       $a.ida as a_ida, $a.a1, 
       $c.ida as c_ida, $c.idb as c_idb, $c.idc as c_idc,
       row_storage_size($a) < row_storage_size($b) and
       row_storage_size($b) < row_storage_size($c) as c_size,
       20 < index_storage_size($a, "a_idx_a2") and
            index_storage_size($a, "a_idx_a2") < 30 as size_a_idx_a2 
from nested tables(A.B $b ancestors(A $a) descendants(A.B.C $c))
where $b.ida != 40
order by $b.ida, $b.idb
