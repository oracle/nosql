update Foo f
json merge f.info with patch { "a" : [ "b", "c" ] }
where id = 32

select * from foo where id = 32
