update boo $f
set age = $ + 3,
add $f.address.phones seq_concat({ "work" : 3445, "home" : 1231423 },
                                 { "work" : 3446, "home" : 1231423 },
                                 { "work" : 3447, "home" : 1231423 })
where id = 3
returning age,
          $f.address,
          (abs(row_storage_size($f) - 335) <= 1) as row_size,
          index_storage_size($f, "idx_city_phones") as isize_cp,
          index_storage_size($f, "idx_state_city_age") as isize_sca,
          partition($f) as part,
          shard($f) as shard,
          remaining_days($f) as expiration,
          modification_time($f) > current_time() as mod_time
