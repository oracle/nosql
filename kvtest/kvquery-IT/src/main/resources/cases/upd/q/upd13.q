update Foo f
set f.info.address.state = "CA",
set f.info.address.phones = $[3]
where id = 12
returning f.info.address
