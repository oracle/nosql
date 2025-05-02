#Test Description: expression includes exists operator for source
#Expected result: return error

select * from playerinfo p where regex_like(exists p.name,"a.*","i")