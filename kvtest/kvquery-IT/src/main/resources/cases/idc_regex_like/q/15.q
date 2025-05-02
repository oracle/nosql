#Test Description: expression includes is of type operator for source
#Expected result: return error

select * from playerinfo p where regex_like(p.name is of type (string),"a.*","i")