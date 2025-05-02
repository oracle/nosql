update Foo f
remove f.info.maps.values($value = 10 or $value = 20),
remove f.info.children.keys()
where id = 14
returning *
