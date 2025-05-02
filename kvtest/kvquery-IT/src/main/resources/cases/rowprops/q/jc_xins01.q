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
returning row_storage_size($f) as row_size,
          index_storage_size($f, "idx_state_city_age") as sca_size
