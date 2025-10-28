update foo $f
set age = $ + 3,
add $f.address.phones seq_concat({ "work" : 3445, "home" : 1231423 },
                                 { "work" : 3446, "home" : 1231423 },
                                 { "work" : 3447, "home" : 1231423 })
where id = 3
returning age,
          $f.address,
          (abs(row_storage_size($f) - 172) <= 1) as row_size,
          index_storage_size($f, "idx_city_phones") as isize_cp,
          index_storage_size($f, "idx_state_city_age") as isize_sca,
          partition($f) as part,
          shard($f) as shard,
          remaining_days($f) as expiration,
          creation_time($f) >= '2020-9-1'  as creation_time,
          creation_time_millis($f) > 1700000000 as creation_ms,
          modification_time($f) >= '2020-9-1' as mod_time
