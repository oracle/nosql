#TestDescription: embedded flag expression should result in error

select regex_like(p.profile,"(?i)test") from playerinfo p