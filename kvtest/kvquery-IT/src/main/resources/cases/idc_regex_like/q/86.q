#TestDescription: Postfix condition  "a.*"  Finds any values that start with "a"

select id1,p.profile from playerinfo p where regex_like(p.profile,"a.*","i")