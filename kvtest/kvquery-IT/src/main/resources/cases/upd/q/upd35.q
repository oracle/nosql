update Foo f
set f.record.map1 =
    { "foo" : { "f1" : { "f1f1" : 1 }, 
                "f2" : { "f2f1" : 4, "f2f2" : parse_json($json) } 
              } 
    }
where id = 29
returning *
