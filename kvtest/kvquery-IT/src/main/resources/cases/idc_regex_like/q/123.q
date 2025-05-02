#TestDescription: embedded flag expression should result in error

select regex_like(p.profile,"(?x)test") from playerinfo p