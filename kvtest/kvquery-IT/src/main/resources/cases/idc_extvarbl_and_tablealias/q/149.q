# Test Description: Reference to external defined variable of type string.

declare $str1 string;
select * from Users where firstname = $str1
order by id