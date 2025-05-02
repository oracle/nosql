#Test Description: expression includes is null operator for source
#Expected result: return error

select * from playerinfo p where regex_like(p.name is null,"a.*","i")