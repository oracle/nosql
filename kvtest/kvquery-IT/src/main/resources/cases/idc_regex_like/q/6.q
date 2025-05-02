#Test discription: Use arithmetic expression for source
#Expected result : return error

select * from playerinfo p where regex_like(cast(p.name as integer) + 100, "1.*")