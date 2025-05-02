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
returning row_storage_size($f) as row_size,
          partition($f) as part,
          shard($f) as shard,
          remaining_days($f) as expiration,
          modification_time($f) > current_time() as mod_time
