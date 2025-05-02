# Test for Flag = "u" UNICODE_CASE

select id1, profile from playerinfo p where regex_like(profile,"\\u004D","u")