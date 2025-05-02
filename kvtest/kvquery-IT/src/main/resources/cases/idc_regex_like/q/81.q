#TestDescription: ".*ar." Find names containing ar, begins with any number of characters, and ends with at most one character

select id1,name from playerinfo p where regex_like(name,".*ar.")