#Test Description: expression includes size() function for source
#Expected result: return error

select * from playerinfo p where regex_like(size(p.name),"a.*","i")