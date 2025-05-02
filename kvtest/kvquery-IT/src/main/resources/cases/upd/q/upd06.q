declare $income integer;
        $number integer;
update Foo f
put f.info.children 
    seq_concat({"Matt" : { "age" : 14, "school" : "sch_2", "friends" : ["Bill"]}},
               {"Dave" : null},
               {"George" : {"age" : $.Tim.age}}),
set record = { },
json merge f.info with patch { "address" : { "city" : $city,
                                             $state : "CA",
                                             "phones" : { "areacode" : 610,
                                                          "number" : $number,
                                                          "kind" : "home" }
                                           },
                               "income" : $income
                             }
where id = 5
returning *
