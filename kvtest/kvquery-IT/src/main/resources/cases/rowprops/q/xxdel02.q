delete from foo $f
where index_storage_size($f, "idx_state_city_age") > 40
returning id,
          row_storage_size($f) as row_size,
          index_storage_size($f, "idx_state_city_age") as isize_sca,
          index_storage_size($f, "idx_city_phones") as isize_cp,
          partition($f) as part,
          shard($f) as shard,
          remaining_days($f) as expiration,
          modification_time($f) > current_time() as mod_time
