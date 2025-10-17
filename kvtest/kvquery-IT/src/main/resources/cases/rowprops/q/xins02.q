insert into foo $f values (100, "first100", "last100", 33, "lastName",

  {
    "city": "San Fransisco",
    "state"  : "CA",
    "phones" : [ { "work" : 504,  "home" : 50 },
                 { "work" : 518,  "home" : 51 },
                 { "work" : 528,  "home" : 52 },
                 { "work" : 538,  "home" : 53 },
                 { "work" : 548,  "home" : 54 } ],
    "ptr"    : "city"
  },
  {}
)
returning (abs(row_storage_size($f) - 166) <= 1) as row_size,
          partition($f) as part,
          shard($f) as shard,
          remaining_days($f) as expiration,
          creation_time($f) >= "2020-9-1" as creation_time,
          creation_time_millis($f) > 1700000000 as creation_ms,
          modification_time($f) >= '2020-9-1'  as mod_time
