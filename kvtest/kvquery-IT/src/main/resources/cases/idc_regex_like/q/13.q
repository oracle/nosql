#Test Description: expression includes is not null operator for source
#Expected result: return error

select * from playerinfo p where regex_like(p.name is not null,"a.*","i")