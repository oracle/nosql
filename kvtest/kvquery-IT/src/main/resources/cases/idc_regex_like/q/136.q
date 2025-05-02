# Test for Flag = "x" COMMENTS

select id1, profile from playerinfo p where regex_like(profile,"Test#comment\n","x")