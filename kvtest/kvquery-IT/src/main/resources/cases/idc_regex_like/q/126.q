#TestDescription: embedded flag expression should result in error

select regex_like(p.profile,"(?U)test") from playerinfo p