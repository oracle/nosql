#TestDescription: embedded flag expression should result in error

select regex_like(p.profile,"(?d)test") from playerinfo p