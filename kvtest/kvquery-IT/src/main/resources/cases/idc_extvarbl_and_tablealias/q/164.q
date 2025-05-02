# Test Description: Literal table alias with valid characters.

select id, record.select from 
select record 
order by id