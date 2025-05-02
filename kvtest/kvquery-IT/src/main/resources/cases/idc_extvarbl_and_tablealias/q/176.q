# Test Description: Test for external variable of type BOOEAN.


declare $boolfalse boolean;
select * from Users where married = $boolfalse
order by id