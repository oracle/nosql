#TestDescription: ".ar.*" Find names containing ar, begins with at most one character, and ends with any number of characters

select id1,p.info.id.name from playerinfo p where regex_like(p.info.id.name,".ar.*")