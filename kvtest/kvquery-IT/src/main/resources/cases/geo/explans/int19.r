compiled-query-plan

{
"query file" : "geo/q/int19.q",
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
          "range conditions" : { "info.point" : { "start value" : "sw28n", "start inclusive" : true, "end value" : "sw28rzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw28w", "start inclusive" : true, "end value" : "sw28zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw29n", "start inclusive" : true, "end value" : "sw29rzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw29w", "start inclusive" : true, "end value" : "sw2czzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2dn", "start inclusive" : true, "end value" : "sw2drzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2dw", "start inclusive" : true, "end value" : "sw2dzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2en", "start inclusive" : true, "end value" : "sw2epzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2f0", "start inclusive" : true, "end value" : "sw2g1zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2g4", "start inclusive" : true, "end value" : "sw2g5zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2gh", "start inclusive" : true, "end value" : "sw2gjzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2gn", "start inclusive" : true, "end value" : "sw2gpzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw300", "start inclusive" : true, "end value" : "sw320zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw322", "start inclusive" : true, "end value" : "sw322", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw328", "start inclusive" : true, "end value" : "sw32gzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw32s", "start inclusive" : true, "end value" : "sw32wzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw32y", "start inclusive" : true, "end value" : "sw32y", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw330", "start inclusive" : true, "end value" : "sw33nzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33q", "start inclusive" : true, "end value" : "sw33q", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s", "start inclusive" : true, "end value" : "sw33wzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33y", "start inclusive" : true, "end value" : "sw33y", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw340", "start inclusive" : true, "end value" : "sw351zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw354", "start inclusive" : true, "end value" : "sw355zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw35h", "start inclusive" : true, "end value" : "sw35jzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw35n", "start inclusive" : true, "end value" : "sw35pzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw360", "start inclusive" : true, "end value" : "sw362zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw364", "start inclusive" : true, "end value" : "sw365zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw368", "start inclusive" : true, "end value" : "sw368", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw36b", "start inclusive" : true, "end value" : "sw36b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw36h", "start inclusive" : true, "end value" : "sw36jzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw36n", "start inclusive" : true, "end value" : "sw36n", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw370", "start inclusive" : true, "end value" : "sw370", "end inclusive" : true } }
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
        "value" : {"coordinates":[[[[23.9253,35.269],[24.5473,35.2937],[24.513,35.39],[23.9555,35.5365],[23.9253,35.269]]],[[[23.48,35.16],[24.3,35.16],[24.3,35.7],[23.48,35.7],[23.48,35.16]]]],"type":"MultiPolygon"}
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