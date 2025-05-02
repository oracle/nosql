update Foo f
json merge f.info with patch 5
where id = 31

select * from foo where id = 31
