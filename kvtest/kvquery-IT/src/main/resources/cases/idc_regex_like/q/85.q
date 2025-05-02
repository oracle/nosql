#TestDescription: ".*%.*"  to seek columns that contain a percent sign (%)

select id1,p.profile from playerinfo p where regex_like(p.profile,".*%.*" )