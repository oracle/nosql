select  [ d.d4[size($) - 2 : ] ] as lastTwoStrings, a.ida1 as a_ida1 from A a left outer join A.B.D d on a.ida1 = d.ida1 where d.d3<1000
