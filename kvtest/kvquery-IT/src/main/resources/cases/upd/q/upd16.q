update Foo f
put f.info.map f.record
where id = 15
returning f.info.map
