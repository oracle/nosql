# Test for Flag = "s" DOTALL 

select id1,profile from playerinfo where regex_like(profile,"This.*sentence","s")