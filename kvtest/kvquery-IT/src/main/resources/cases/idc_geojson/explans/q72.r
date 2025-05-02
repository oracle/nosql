compiled-query-plan

{
"query file" : "idc_geojson/q/q72.q",
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
        "index used" : "idx_kind_ptn",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9vz", "start inclusive" : true, "end value" : "t9vz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9yp", "start inclusive" : true, "end value" : "t9yp", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9yr", "start inclusive" : true, "end value" : "t9yr", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9yx", "start inclusive" : true, "end value" : "t9yx", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9yz", "start inclusive" : true, "end value" : "t9yz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9zp", "start inclusive" : true, "end value" : "t9zp", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9zr", "start inclusive" : true, "end value" : "t9zr", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9zx", "start inclusive" : true, "end value" : "t9zx", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "t9zz", "start inclusive" : true, "end value" : "t9zz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tcbp", "start inclusive" : true, "end value" : "tcbp", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tcbr", "start inclusive" : true, "end value" : "tcbr", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdjb", "start inclusive" : true, "end value" : "tdjczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdjf", "start inclusive" : true, "end value" : "tdjgzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdju", "start inclusive" : true, "end value" : "tdjvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdjy", "start inclusive" : true, "end value" : "tdjzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdmb", "start inclusive" : true, "end value" : "tdmczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdmf", "start inclusive" : true, "end value" : "tdmgzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdmu", "start inclusive" : true, "end value" : "tdmvzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdmy", "start inclusive" : true, "end value" : "tdrzzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdtb", "start inclusive" : true, "end value" : "tdtczzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdtf", "start inclusive" : true, "end value" : "tdtgzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdtu", "start inclusive" : true, "end value" : "tdtu", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdw0", "start inclusive" : true, "end value" : "tdwhzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdwk", "start inclusive" : true, "end value" : "tdwk", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdws", "start inclusive" : true, "end value" : "tdws", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdwu", "start inclusive" : true, "end value" : "tdwu", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdx0", "start inclusive" : true, "end value" : "tdxhzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdxk", "start inclusive" : true, "end value" : "tdxk", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdxs", "start inclusive" : true, "end value" : "tdxs", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tdxu", "start inclusive" : true, "end value" : "tdxu", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf00", "start inclusive" : true, "end value" : "tf07zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf0h", "start inclusive" : true, "end value" : "tf0rzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf20", "start inclusive" : true, "end value" : "tf27zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf2h", "start inclusive" : true, "end value" : "tf2rzzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf80", "start inclusive" : true, "end value" : "tf87zzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf8h", "start inclusive" : true, "end value" : "tf8h", "end inclusive" : true } }
          },
          {
            "equality conditions" : {"info.kind":"port"},
            "range conditions" : { "info.point" : { "start value" : "tf8k", "start inclusive" : true, "end value" : "tf8k", "end inclusive" : true } }
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
          "value" : {"coordinates":[[77.5909423828125,12.983147716796578],[77.55575180053711,13.012000642911662]],"type":"multipoint"}
        },
        "distance iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 200000.098
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
              "value" : {"coordinates":[[77.5909423828125,12.983147716796578],[77.55575180053711,13.012000642911662]],"type":"multipoint"}
            }
          }
        }
      ]
    }
  }
}
}