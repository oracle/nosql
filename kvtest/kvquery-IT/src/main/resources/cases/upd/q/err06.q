update Foo f
set f.info.age = max(f.id)
where id = 0
