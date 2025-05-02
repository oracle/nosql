compiled-query-plan

{
"query file" : "geo/q/int05.q",
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
          "range conditions" : { "info.point" : { "start value" : "sw3443myt", "start inclusive" : true, "end value" : "sw3443myt", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443myw", "start inclusive" : true, "end value" : "sw3443myxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qn8", "start inclusive" : true, "end value" : "sw3443qn9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qnd", "start inclusive" : true, "end value" : "sw3443qnez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qns", "start inclusive" : true, "end value" : "sw3443qntz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qnw", "start inclusive" : true, "end value" : "sw3443qnxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qq8", "start inclusive" : true, "end value" : "sw3443qq9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qqd", "start inclusive" : true, "end value" : "sw3443qqez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qqs", "start inclusive" : true, "end value" : "sw3443qqtz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qqw", "start inclusive" : true, "end value" : "sw3443qqxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qw8", "start inclusive" : true, "end value" : "sw3443qw9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qwd", "start inclusive" : true, "end value" : "sw3443qwez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qws", "start inclusive" : true, "end value" : "sw3443qwtz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qww", "start inclusive" : true, "end value" : "sw3443qwxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qy8", "start inclusive" : true, "end value" : "sw3443qy9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qyd", "start inclusive" : true, "end value" : "sw3443qyez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qys", "start inclusive" : true, "end value" : "sw3443qytz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443qyw", "start inclusive" : true, "end value" : "sw3443qyxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rn8", "start inclusive" : true, "end value" : "sw3443rn9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rnd", "start inclusive" : true, "end value" : "sw3443rnez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rns", "start inclusive" : true, "end value" : "sw3443rntz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rnw", "start inclusive" : true, "end value" : "sw3443rnxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rq8", "start inclusive" : true, "end value" : "sw3443rq9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rqd", "start inclusive" : true, "end value" : "sw3443rqez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rqs", "start inclusive" : true, "end value" : "sw3443rqtz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rqw", "start inclusive" : true, "end value" : "sw3443rqxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rw8", "start inclusive" : true, "end value" : "sw3443rw9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rwd", "start inclusive" : true, "end value" : "sw3443rwez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rws", "start inclusive" : true, "end value" : "sw3443rwtz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rww", "start inclusive" : true, "end value" : "sw3443rwxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443ry8", "start inclusive" : true, "end value" : "sw3443ry9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443ryd", "start inclusive" : true, "end value" : "sw3443ryez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443rys", "start inclusive" : true, "end value" : "sw3443rytz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3443ryw", "start inclusive" : true, "end value" : "sw3443ryxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492n8", "start inclusive" : true, "end value" : "sw34492n9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492nd", "start inclusive" : true, "end value" : "sw34492nez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492ns", "start inclusive" : true, "end value" : "sw34492ntz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492nw", "start inclusive" : true, "end value" : "sw34492nxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492q8", "start inclusive" : true, "end value" : "sw34492q9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492qd", "start inclusive" : true, "end value" : "sw34492qez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492qs", "start inclusive" : true, "end value" : "sw34492qtz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492qw", "start inclusive" : true, "end value" : "sw34492qxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492w8", "start inclusive" : true, "end value" : "sw34492w9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492wd", "start inclusive" : true, "end value" : "sw34492wez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492ws", "start inclusive" : true, "end value" : "sw34492wtz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492ww", "start inclusive" : true, "end value" : "sw34492wxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492y8", "start inclusive" : true, "end value" : "sw34492y9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492yd", "start inclusive" : true, "end value" : "sw34492yez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492ys", "start inclusive" : true, "end value" : "sw34492ytz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34492yw", "start inclusive" : true, "end value" : "sw34492yxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493n8", "start inclusive" : true, "end value" : "sw34493n9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493nd", "start inclusive" : true, "end value" : "sw34493nez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493ns", "start inclusive" : true, "end value" : "sw34493ntz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493nw", "start inclusive" : true, "end value" : "sw34493nxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493q8", "start inclusive" : true, "end value" : "sw34493q9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493qd", "start inclusive" : true, "end value" : "sw34493qez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493qs", "start inclusive" : true, "end value" : "sw34493qtz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493qw", "start inclusive" : true, "end value" : "sw34493qxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493w8", "start inclusive" : true, "end value" : "sw34493w9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493wd", "start inclusive" : true, "end value" : "sw34493wez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493ws", "start inclusive" : true, "end value" : "sw34493wtz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493ww", "start inclusive" : true, "end value" : "sw34493wxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493y8", "start inclusive" : true, "end value" : "sw34493y9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493yd", "start inclusive" : true, "end value" : "sw34493yez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493ys", "start inclusive" : true, "end value" : "sw34493ytz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34493yw", "start inclusive" : true, "end value" : "sw34493yxz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34496n8", "start inclusive" : true, "end value" : "sw34496n9z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34496nd", "start inclusive" : true, "end value" : "sw34496nez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34496ns", "start inclusive" : true, "end value" : "sw34496ntz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw34496nw", "start inclusive" : true, "end value" : "sw34496nxz", "end inclusive" : true } }
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
        "value" : {"coordinates":[[24.01327,35.5158],[24.019193,35.5158]],"type":"LineString"}
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