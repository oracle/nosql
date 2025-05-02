#TestDescription: embedded flag expression should result in error

select regex_like(p.profile,"(?u)test") from playerinfo p