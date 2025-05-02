#TestDescription: "Sa." Find names beginning with Sa and is followed by at most one character

select id1,name from playerinfo p where regex_like(name,"Sa.*")