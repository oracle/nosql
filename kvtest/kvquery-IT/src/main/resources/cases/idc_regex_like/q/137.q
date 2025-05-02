# Test for Flag = "l" LITERAL

select id1, profile from playerinfo p where regex_like(profile,"test.**literal","l")