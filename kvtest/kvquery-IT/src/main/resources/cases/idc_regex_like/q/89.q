#TestDescription: Test quoting literals. To match two stars use \\Q**\\E. This will match two literal stars, and not try and interpret them as the "zero or more" quantifier.

select id1,p.profile from playerinfo p where regex_like(p.profile,".*\\Q**\\E.*","i")