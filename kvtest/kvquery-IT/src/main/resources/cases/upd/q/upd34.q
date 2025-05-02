declare $ext1 string;
update Foo f
set f.info.age = $ + 2,
set f.info.lastName = $ext1
where id = 28
returning *

