delete from foo $f
where index_storage_size($f, "idx_city_phones") < 40
returning id,
          (abs(row_storage_size($f) - 182) <= 1) as row_size,
          index_storage_size($f, "idx_city_phones") as isize_cp,
          index_storage_size($f, "idx_state_city_age") as isize_sca,
          partition($f) as part,
          shard($f) as shard,
          remaining_days($f) as expiration,
          creation_time($f) >= '2020-9-1' as creation_time,
          creation_time_millis($f) > 1700000000 as creation_ms,
          modification_time($f) >= '2020-9-1' as mod_time
