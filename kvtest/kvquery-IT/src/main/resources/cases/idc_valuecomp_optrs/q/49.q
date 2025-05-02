# Test Description: Integer column operand compared with float numeric literal using <= operator.

select id1, int, flt from Foo where int <= 3.4028235E38
order by id1