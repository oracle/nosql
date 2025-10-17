insert into boo $f values (
  100, 
  {
    "firstName" : "first100",
    "lastName" : "last100",
    "age" : 33,
    "address" :
      {
        "city": "San Fransisco",
        "state"  : "CA",
        "phones" : [ { "work" : 504,  "home" : 50 },
                     { "work" : 518,  "home" : 51 },
                     { "work" : 528,  "home" : 52 },
                     { "work" : 538,  "home" : 53 },
                     { "work" : 548,  "home" : 54 } ]
      }
   }
)
returning (abs(row_storage_size($f) - 320) <= 1) as row_size,
          partition($f) as part,
          shard($f) as shard,
          remaining_days($f) as expiration,
          modification_time($f) > current_time() as mod_time
