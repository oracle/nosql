# Test Description: Long column operand compared with float literal using <= operator.

select id1, lng, flt from Foo where lng <= 3.4028235E38
order by id1