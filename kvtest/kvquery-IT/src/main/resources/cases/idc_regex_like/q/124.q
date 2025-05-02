#TestDescription: embedded flag expression should result in error

select regex_like(p.profile,"(?s)test") from playerinfo p