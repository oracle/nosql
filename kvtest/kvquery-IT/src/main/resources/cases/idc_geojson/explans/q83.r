compiled-query-plan

{
"query file" : "idc_geojson/q/q83.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 0, 1 ],
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
            "range conditions" : { "info.point" : { "start value" : "t9v4", "start inclusive" : true, "end value" : "t9v7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9vd", "start inclusive" : true, "end value" : "t9vzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9y4", "start inclusive" : true, "end value" : "t9y7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9yd", "start inclusive" : true, "end value" : "t9yzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9z4", "start inclusive" : true, "end value" : "t9z7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "t9zd", "start inclusive" : true, "end value" : "t9zzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcb4", "start inclusive" : true, "end value" : "tcb7zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcbd", "start inclusive" : true, "end value" : "tcbzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcc4", "start inclusive" : true, "end value" : "tcc5zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tcch", "start inclusive" : true, "end value" : "tccjzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tccn", "start inclusive" : true, "end value" : "tccpzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdj0", "start inclusive" : true, "end value" : "tdjzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdm0", "start inclusive" : true, "end value" : "tdrzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdt0", "start inclusive" : true, "end value" : "tdtzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdv0", "start inclusive" : true, "end value" : "tdv3zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdv8", "start inclusive" : true, "end value" : "tdvczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdw0", "start inclusive" : true, "end value" : "tdy3zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdy8", "start inclusive" : true, "end value" : "tdyczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdz0", "start inclusive" : true, "end value" : "tdz3zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tdz8", "start inclusive" : true, "end value" : "tdzczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf00", "start inclusive" : true, "end value" : "tf11zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf14", "start inclusive" : true, "end value" : "tf15zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf1h", "start inclusive" : true, "end value" : "tf1jzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf1n", "start inclusive" : true, "end value" : "tf1pzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf20", "start inclusive" : true, "end value" : "tf31zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf34", "start inclusive" : true, "end value" : "tf35zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf3h", "start inclusive" : true, "end value" : "tf3jzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf3n", "start inclusive" : true, "end value" : "tf3pzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf80", "start inclusive" : true, "end value" : "tf91zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf94", "start inclusive" : true, "end value" : "tf95zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf9h", "start inclusive" : true, "end value" : "tf9jzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tf9n", "start inclusive" : true, "end value" : "tf9pzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tfb0", "start inclusive" : true, "end value" : "tfb3zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tfb8", "start inclusive" : true, "end value" : "tfbczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.point" : { "start value" : "tfc0", "start inclusive" : true, "end value" : "tfc1zzzzzz", "end inclusive" : true } }
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
          "value" : {"coordinates":[[77.57508516311646,12.972943850854447],[77.57585763931274,12.971877439057483],[77.57607221603394,12.970957393833265],[77.57607221603394,12.970246447465623],[77.57686614990234,12.969012741593874],[77.57871150970459,12.969305485913676],[77.58057832717896,12.970058255439973],[77.5820803642273,12.969075472548546]],"type":"LineString"}
        },
        "distance iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 300000.098
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
              "value" : {"coordinates":[[77.57508516311646,12.972943850854447],[77.57585763931274,12.971877439057483],[77.57607221603394,12.970957393833265],[77.57607221603394,12.970246447465623],[77.57686614990234,12.969012741593874],[77.57871150970459,12.969305485913676],[77.58057832717896,12.970058255439973],[77.5820803642273,12.969075472548546]],"type":"LineString"}
            }
          }
        }
      ]
    }
  }
}
}