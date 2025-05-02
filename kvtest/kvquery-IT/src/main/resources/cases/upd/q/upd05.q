update Foo f
add f.info.address.phones 2 seq_concat("650-345-6798", "408-589-3456"),
set f.info.phones[0].number = f.record.long,
json merge f.info.children.Anna.friends with patch { "Mark" : 3, "John" : null }
where id = 4
returning f.info
