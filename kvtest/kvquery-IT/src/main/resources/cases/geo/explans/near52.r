compiled-query-plan

{
"query file" : "geo/q/near52.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 2 ],
  "input iterator" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "distinct by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "polygons",
        "row variable" : "$$p",
        "index used" : "idx_geom",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw312n", "start inclusive" : true, "end value" : "sw312rzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw312w", "start inclusive" : true, "end value" : "sw312zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw313n", "start inclusive" : true, "end value" : "sw313rzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw313w", "start inclusive" : true, "end value" : "sw313zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw316n", "start inclusive" : true, "end value" : "sw316rzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw316w", "start inclusive" : true, "end value" : "sw316zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw317n", "start inclusive" : true, "end value" : "sw317rzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw317w", "start inclusive" : true, "end value" : "sw31gzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31kn", "start inclusive" : true, "end value" : "sw31krzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31kw", "start inclusive" : true, "end value" : "sw31kzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31mn", "start inclusive" : true, "end value" : "sw31mpzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31s0", "start inclusive" : true, "end value" : "sw31t1zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31t4", "start inclusive" : true, "end value" : "sw31t5zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31th", "start inclusive" : true, "end value" : "sw31tjzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31tn", "start inclusive" : true, "end value" : "sw31tpzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31u0", "start inclusive" : true, "end value" : "sw31v1zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31v4", "start inclusive" : true, "end value" : "sw31v5zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31vh", "start inclusive" : true, "end value" : "sw31vjzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw31vn", "start inclusive" : true, "end value" : "sw31vpzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw3400", "start inclusive" : true, "end value" : "sw3483zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw3488", "start inclusive" : true, "end value" : "sw348czzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw3490", "start inclusive" : true, "end value" : "sw3493zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw3498", "start inclusive" : true, "end value" : "sw349czzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34d0", "start inclusive" : true, "end value" : "sw34d3zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34d8", "start inclusive" : true, "end value" : "sw34dczzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34e0", "start inclusive" : true, "end value" : "sw34e3zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34e8", "start inclusive" : true, "end value" : "sw34eczzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34h0", "start inclusive" : true, "end value" : "sw34j1zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34j4", "start inclusive" : true, "end value" : "sw34j5zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34jh", "start inclusive" : true, "end value" : "sw34jjzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34jn", "start inclusive" : true, "end value" : "sw34jpzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34k0", "start inclusive" : true, "end value" : "sw34m1zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34m4", "start inclusive" : true, "end value" : "sw34m5zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34mh", "start inclusive" : true, "end value" : "sw34mjzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34mn", "start inclusive" : true, "end value" : "sw34mpzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34s0", "start inclusive" : true, "end value" : "sw34s3zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34s8", "start inclusive" : true, "end value" : "sw34sczzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw34t0", "start inclusive" : true, "end value" : "sw34t1zzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.geom":"s"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw3"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw312"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw313"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw316"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw317"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31g"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31k"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31m"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31s"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31t"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31u"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31v"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw340"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw348"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw349"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34d"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34e"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34h"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34j"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34k"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34m"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34s"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34t"},
            "range conditions" : {}
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
          "field name" : "geom",
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
          "value" : {"coordinates":[[24.0262,35.5043],[24.0202,35.5122]],"type":"linestring"}
        },
        "distance iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 10000.0
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
          "field name" : "geom",
          "field expression" : 
          {
            "iterator kind" : "ARRAY_CONSTRUCTOR",
            "conditional" : true,
            "input iterators" : [
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "geom",
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
        },
        {
          "field name" : "Column_3",
          "field expression" : 
          {
            "iterator kind" : "GEO_DISTANCE",
            "first geometry iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "geom",
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
              "value" : {"coordinates":[[24.0262,35.5043],[24.0202,35.5122]],"type":"linestring"}
            }
          }
        }
      ]
    }
  }
}
}