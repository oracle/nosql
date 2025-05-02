# Test Description: The names of variables are case-sensitive. Actual variable name is $str and we are providing $STR.


declare $STR string;
select * from Users where firstname = $STR
order by id