select row_storage_size($t) as Row_Storage_Size,
       index_storage_size($t,"idx_t_user1") as IDX_SIZE
from t_user1 $t
