# Test Description: Variablename alias with valid characters.

select id, firstName AS F_NAM_E, lastName AS mn98_NAME, age AS A_G345 
From Users 
Where age > 18
order by id