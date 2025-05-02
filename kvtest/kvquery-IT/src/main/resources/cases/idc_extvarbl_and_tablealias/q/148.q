# Test Description: Reference to external defined variable of type string.

declare $str0 string;
select * from Users where lastname = $str0
order by id
