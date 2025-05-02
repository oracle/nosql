# Test Description: Test for external variable of type BOOEAN.


declare $booltrue boolean;
select * from Users where married = $booltrue
order by id