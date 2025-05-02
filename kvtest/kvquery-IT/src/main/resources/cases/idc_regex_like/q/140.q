# Test for Flag = Pattern.CASE_INSENSITIVE + Pattern.UNICODE_CASE

select id1, profile from playerinfo p where regex_like(name,"\\u004D","ui")