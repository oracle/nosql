#TestDescription: ".an." Find names containing an and begins and ends with at most one character

select id1,name from playerinfo p where regex_like(name,".an.")