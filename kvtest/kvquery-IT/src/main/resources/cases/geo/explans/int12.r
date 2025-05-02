compiled-query-plan

{
"query file" : "geo/q/int12.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "points",
      "row variable" : "$$p",
      "index used" : "idx_ptn",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h00", "start inclusive" : true, "end value" : "h00", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h02", "start inclusive" : true, "end value" : "h02", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h08", "start inclusive" : true, "end value" : "h08", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h0b", "start inclusive" : true, "end value" : "h0b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h10", "start inclusive" : true, "end value" : "h10", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h12", "start inclusive" : true, "end value" : "h12", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h18", "start inclusive" : true, "end value" : "h18", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h1b", "start inclusive" : true, "end value" : "h1b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h40", "start inclusive" : true, "end value" : "h40", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h42", "start inclusive" : true, "end value" : "h42", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h48", "start inclusive" : true, "end value" : "h48", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h4b", "start inclusive" : true, "end value" : "h4b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h50", "start inclusive" : true, "end value" : "h50", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h52", "start inclusive" : true, "end value" : "h52", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h58", "start inclusive" : true, "end value" : "h58", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "h5b", "start inclusive" : true, "end value" : "h5b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hh0", "start inclusive" : true, "end value" : "hh0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hh2", "start inclusive" : true, "end value" : "hh2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hh8", "start inclusive" : true, "end value" : "hh8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hhb", "start inclusive" : true, "end value" : "hhb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hj0", "start inclusive" : true, "end value" : "hj0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hj2", "start inclusive" : true, "end value" : "hj2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hj8", "start inclusive" : true, "end value" : "hj8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hjb", "start inclusive" : true, "end value" : "hjb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hn0", "start inclusive" : true, "end value" : "hn0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hn2", "start inclusive" : true, "end value" : "hn2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hn8", "start inclusive" : true, "end value" : "hn8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hnb", "start inclusive" : true, "end value" : "hnb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hp0", "start inclusive" : true, "end value" : "hp0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hp2", "start inclusive" : true, "end value" : "hp2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hp8", "start inclusive" : true, "end value" : "hp8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "hpb", "start inclusive" : true, "end value" : "hpb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k00", "start inclusive" : true, "end value" : "k00", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k02", "start inclusive" : true, "end value" : "k02", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k08", "start inclusive" : true, "end value" : "k08", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k0b", "start inclusive" : true, "end value" : "k0b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k10", "start inclusive" : true, "end value" : "k10", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k12", "start inclusive" : true, "end value" : "k12", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k18", "start inclusive" : true, "end value" : "k18", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k1b", "start inclusive" : true, "end value" : "k1b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k40", "start inclusive" : true, "end value" : "k40", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k42", "start inclusive" : true, "end value" : "k42", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k48", "start inclusive" : true, "end value" : "k48", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k4b", "start inclusive" : true, "end value" : "k4b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k50", "start inclusive" : true, "end value" : "k50", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k52", "start inclusive" : true, "end value" : "k52", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k58", "start inclusive" : true, "end value" : "k58", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "k5b", "start inclusive" : true, "end value" : "k5b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kh0", "start inclusive" : true, "end value" : "kh0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kh2", "start inclusive" : true, "end value" : "kh2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kh8", "start inclusive" : true, "end value" : "kh8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "khb", "start inclusive" : true, "end value" : "khb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kj0", "start inclusive" : true, "end value" : "kj0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kj2", "start inclusive" : true, "end value" : "kj2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kj8", "start inclusive" : true, "end value" : "kj8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kjb", "start inclusive" : true, "end value" : "kjb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kn0", "start inclusive" : true, "end value" : "kn0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kn2", "start inclusive" : true, "end value" : "kn2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kn8", "start inclusive" : true, "end value" : "kn8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "knb", "start inclusive" : true, "end value" : "knb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kp0", "start inclusive" : true, "end value" : "kp0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kp2", "start inclusive" : true, "end value" : "kp2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kp8", "start inclusive" : true, "end value" : "kp8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "kpb", "start inclusive" : true, "end value" : "kpb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s00", "start inclusive" : true, "end value" : "s00", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s02", "start inclusive" : true, "end value" : "s02", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s08", "start inclusive" : true, "end value" : "s08", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s0b", "start inclusive" : true, "end value" : "s0b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s10", "start inclusive" : true, "end value" : "s10", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s12", "start inclusive" : true, "end value" : "s12", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s18", "start inclusive" : true, "end value" : "s18", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s1b", "start inclusive" : true, "end value" : "s1b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s40", "start inclusive" : true, "end value" : "s40", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s42", "start inclusive" : true, "end value" : "s42", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s48", "start inclusive" : true, "end value" : "s48", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s4b", "start inclusive" : true, "end value" : "s4b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s50", "start inclusive" : true, "end value" : "s50", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s52", "start inclusive" : true, "end value" : "s52", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s58", "start inclusive" : true, "end value" : "s58", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "s5b", "start inclusive" : true, "end value" : "s5b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sh0", "start inclusive" : true, "end value" : "sh0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sh2", "start inclusive" : true, "end value" : "sh2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sh8", "start inclusive" : true, "end value" : "sh8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "shb", "start inclusive" : true, "end value" : "shb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sj0", "start inclusive" : true, "end value" : "sj0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sj2", "start inclusive" : true, "end value" : "sj2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sj8", "start inclusive" : true, "end value" : "sj8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sjb", "start inclusive" : true, "end value" : "sjb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sn0", "start inclusive" : true, "end value" : "sn0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sn2", "start inclusive" : true, "end value" : "sn2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sn8", "start inclusive" : true, "end value" : "sn8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "snb", "start inclusive" : true, "end value" : "snb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sp0", "start inclusive" : true, "end value" : "sp0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sp2", "start inclusive" : true, "end value" : "sp2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sp8", "start inclusive" : true, "end value" : "sp8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "spb", "start inclusive" : true, "end value" : "spb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u00", "start inclusive" : true, "end value" : "u00", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u02", "start inclusive" : true, "end value" : "u02", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u08", "start inclusive" : true, "end value" : "u08", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u0b", "start inclusive" : true, "end value" : "u0b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u10", "start inclusive" : true, "end value" : "u10", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u12", "start inclusive" : true, "end value" : "u12", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u18", "start inclusive" : true, "end value" : "u18", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u1b", "start inclusive" : true, "end value" : "u1b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u40", "start inclusive" : true, "end value" : "u40", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u42", "start inclusive" : true, "end value" : "u42", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u48", "start inclusive" : true, "end value" : "u48", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u4b", "start inclusive" : true, "end value" : "u4b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u50", "start inclusive" : true, "end value" : "u50", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u52", "start inclusive" : true, "end value" : "u52", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u58", "start inclusive" : true, "end value" : "u58", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "u5b", "start inclusive" : true, "end value" : "u5b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "uh0", "start inclusive" : true, "end value" : "uh0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "uh2", "start inclusive" : true, "end value" : "uh2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "uh8", "start inclusive" : true, "end value" : "uh8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "uhb", "start inclusive" : true, "end value" : "uhb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "uj0", "start inclusive" : true, "end value" : "uj0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "uj2", "start inclusive" : true, "end value" : "uj2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "uj8", "start inclusive" : true, "end value" : "uj8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ujb", "start inclusive" : true, "end value" : "ujb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "un0", "start inclusive" : true, "end value" : "un0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "un2", "start inclusive" : true, "end value" : "un2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "un8", "start inclusive" : true, "end value" : "un8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "unb", "start inclusive" : true, "end value" : "unb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "up0", "start inclusive" : true, "end value" : "up0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "up2", "start inclusive" : true, "end value" : "up2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "up8", "start inclusive" : true, "end value" : "up8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "upb", "start inclusive" : true, "end value" : "upb", "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "WHERE" : 
    {
      "iterator kind" : "FN_GEO_INTERSECT",
      "search target iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "point",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$p"
          }
        }
      },
      "search geometry iterator" :
      {
        "iterator kind" : "CONST",
        "value" : {"coordinates":[[0,-90],[0,90]],"type":"LineString"}
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$p"
          }
        }
      },
      {
        "field name" : "point",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "point",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$p"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}