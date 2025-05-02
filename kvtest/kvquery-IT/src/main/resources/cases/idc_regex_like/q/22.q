#Test Description: expression includes count() function call for source
#Expected result: return error

select regex_like(count(p.name), "v.*","i") from playerinfo p