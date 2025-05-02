#
# Insert existing row
#
insert into foo $f values (
    "gtm", 
    1, 
    100, 
    {
      "a" : 10,
      "b" : 30 
    },
    default
)
returning id1, id2, 
          120 <= remaining_hours($f) and remaining_hours($f) < 144,
          135 <= row_storage_size($f) and row_storage_size($f) <= 145 as row_size
