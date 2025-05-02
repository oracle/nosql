# Test Description: Reference to external defined variable which is not initialized.

select * from Users where id = $v1
order by id