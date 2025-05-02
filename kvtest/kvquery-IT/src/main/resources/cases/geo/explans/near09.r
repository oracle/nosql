compiled-query-plan

{
"query file" : "geo/q/near09.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 1 ],
  "input iterator" :
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
            "range conditions" : { "info.point" : { "start value" : "18gf", "start inclusive" : true, "end value" : "18ggzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18gu", "start inclusive" : true, "end value" : "18gvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18u4", "start inclusive" : true, "end value" : "18u7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18ud", "start inclusive" : true, "end value" : "18umzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18us", "start inclusive" : true, "end value" : "18uvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18v4", "start inclusive" : true, "end value" : "18v7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18vd", "start inclusive" : true, "end value" : "18vmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18vs", "start inclusive" : true, "end value" : "18vvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18y4", "start inclusive" : true, "end value" : "18y7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18yd", "start inclusive" : true, "end value" : "18ymzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18ys", "start inclusive" : true, "end value" : "18yvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18z4", "start inclusive" : true, "end value" : "18z7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18zd", "start inclusive" : true, "end value" : "18zmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "18zs", "start inclusive" : true, "end value" : "18zvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bb4", "start inclusive" : true, "end value" : "1bb7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bbd", "start inclusive" : true, "end value" : "1bbmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bbs", "start inclusive" : true, "end value" : "1bbvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bc4", "start inclusive" : true, "end value" : "1bc7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bcd", "start inclusive" : true, "end value" : "1bcmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bcs", "start inclusive" : true, "end value" : "1bcvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bf4", "start inclusive" : true, "end value" : "1bf7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bfd", "start inclusive" : true, "end value" : "1bfmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bfs", "start inclusive" : true, "end value" : "1bfvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bg4", "start inclusive" : true, "end value" : "1bg7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bgd", "start inclusive" : true, "end value" : "1bgmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bgs", "start inclusive" : true, "end value" : "1bgvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bu4", "start inclusive" : true, "end value" : "1bu7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bud", "start inclusive" : true, "end value" : "1bumzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bus", "start inclusive" : true, "end value" : "1buvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bv4", "start inclusive" : true, "end value" : "1bv7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bvd", "start inclusive" : true, "end value" : "1bvmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bvs", "start inclusive" : true, "end value" : "1bvvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1by4", "start inclusive" : true, "end value" : "1by7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1byd", "start inclusive" : true, "end value" : "1bymzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bys", "start inclusive" : true, "end value" : "1byvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bz4", "start inclusive" : true, "end value" : "1bz7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bzd", "start inclusive" : true, "end value" : "1bzmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "1bzs", "start inclusive" : true, "end value" : "1bzvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40b4", "start inclusive" : true, "end value" : "40b7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40bd", "start inclusive" : true, "end value" : "40bmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40bs", "start inclusive" : true, "end value" : "40bvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40c4", "start inclusive" : true, "end value" : "40c7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40cd", "start inclusive" : true, "end value" : "40cmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40cs", "start inclusive" : true, "end value" : "40cvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40f4", "start inclusive" : true, "end value" : "40f7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40fd", "start inclusive" : true, "end value" : "40fmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40fs", "start inclusive" : true, "end value" : "40fvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40g4", "start inclusive" : true, "end value" : "40g7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40gd", "start inclusive" : true, "end value" : "40gmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40gs", "start inclusive" : true, "end value" : "40gvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40u4", "start inclusive" : true, "end value" : "40u7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40ud", "start inclusive" : true, "end value" : "40umzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40us", "start inclusive" : true, "end value" : "40uvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40v4", "start inclusive" : true, "end value" : "40v7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40vd", "start inclusive" : true, "end value" : "40vmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40vs", "start inclusive" : true, "end value" : "40vvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40y4", "start inclusive" : true, "end value" : "40y7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40yd", "start inclusive" : true, "end value" : "40ymzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40ys", "start inclusive" : true, "end value" : "40yvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40z4", "start inclusive" : true, "end value" : "40z7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40zd", "start inclusive" : true, "end value" : "40zmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "40zs", "start inclusive" : true, "end value" : "40zvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "42b4", "start inclusive" : true, "end value" : "42b7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "42bd", "start inclusive" : true, "end value" : "42bezzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "42bh", "start inclusive" : true, "end value" : "42bmzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "42bs", "start inclusive" : true, "end value" : "42btzzzzzz", "end inclusive" : true } }
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$p",
      "WHERE" : 
      {
        "iterator kind" : "FN_GEO_WITHIN_DISTANCE",
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
          "value" : {"coordinates":[[-105.0,-85.0],[-80,-85.0]],"type":"LineString"}
        },
        "distance iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 20000.0
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
          "field name" : "dist",
          "field expression" : 
          {
            "iterator kind" : "GEO_DISTANCE",
            "first geometry iterator" :
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
            "second geometry iterator" :
            {
              "iterator kind" : "CONST",
              "value" : {"coordinates":[[-105.0,-85.0],[-80,-85.0]],"type":"LineString"}
            }
          }
        }
      ]
    }
  }
}
}