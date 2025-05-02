update Foo f
put f.record.map1 
    {
       "rec2" : { "f1" : {},
                  "f2" : { "j1" : [], "j2" : [1, 3], "key3" : "foo" }
                },
       "rec1" : { "f1" : { "i1" : 11 },
                  "f2" : $.rec1.f2
                }
    }
where id = 19
returning f.record.map1
