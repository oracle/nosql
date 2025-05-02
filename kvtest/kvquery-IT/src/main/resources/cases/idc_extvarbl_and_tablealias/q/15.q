# Test Description: Literal alias with invalid characters.

select id AS _ID, firstName AS &NAME, lastName AS (LNAME, age AS A_G345 From Users