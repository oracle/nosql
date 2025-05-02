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
          index_storage_size($f, "idx_state_city_age") as sca_size
