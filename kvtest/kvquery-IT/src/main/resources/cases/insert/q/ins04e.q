insert into foo $f values (
    $f1, 
    1, 
    $id2,
    default,
    {
      "long" : 120,
      "rec2" : { "str" : "dfg", "arr" : [ 10, 4, 6 ] },
      "recinfo" : { "a" : 4, "b" : "str" },
      "map" : { "foo" : "xyz", "bar" : "dff" }
    }
)
set ttl 6 days
returning remaining_days($f),
          148 <= row_storage_size($f) and row_storage_size($f) <= 158 as row_size
