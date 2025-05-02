#TestDescription: ".oy" Find names ending with oy and is preceded by at most one character

select id1,name from playerinfo p where regex_like(name,".oy")