compiled-query-plan

{
"query file" : "idc_geojson/q/q17.q",
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
          "range conditions" : { "info.point" : { "start value" : "tdr1mz3qk", "start inclusive" : true, "end value" : "tdr1mz3qmz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz3qq", "start inclusive" : true, "end value" : "tdr1mz3qzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz3rh", "start inclusive" : true, "end value" : "tdr1mz3rzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz92h", "start inclusive" : true, "end value" : "tdr1mz92zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz93h", "start inclusive" : true, "end value" : "tdr1mz93zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz96h", "start inclusive" : true, "end value" : "tdr1mz96zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz97h", "start inclusive" : true, "end value" : "tdr1mz97zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz9kh", "start inclusive" : true, "end value" : "tdr1mz9kzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz9mh", "start inclusive" : true, "end value" : "tdr1mz9mzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz9qh", "start inclusive" : true, "end value" : "tdr1mz9qzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mz9rh", "start inclusive" : true, "end value" : "tdr1mz9rzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mzc2h", "start inclusive" : true, "end value" : "tdr1mzc2zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mzc3h", "start inclusive" : true, "end value" : "tdr1mzc3zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mzc6h", "start inclusive" : true, "end value" : "tdr1mzc6zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mzc7h", "start inclusive" : true, "end value" : "tdr1mzc7zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mzckh", "start inclusive" : true, "end value" : "tdr1mzckzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mzcmh", "start inclusive" : true, "end value" : "tdr1mzcmzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mzcqh", "start inclusive" : true, "end value" : "tdr1mzcqzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1mzcrh", "start inclusive" : true, "end value" : "tdr1mzcrzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1tb12h", "start inclusive" : true, "end value" : "tdr1tb12zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1tb13h", "start inclusive" : true, "end value" : "tdr1tb13zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1tb16h", "start inclusive" : true, "end value" : "tdr1tb16zz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1tb17h", "start inclusive" : true, "end value" : "tdr1tb17zz", "end inclusive" : true } }
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
        "value" : {"coordinates":[[77.59847402572632,12.920574501916887],[77.59848475456238,12.920516987352773],[77.59843647480011,12.919900012104629],[77.59840965270996,12.91891180281142],[77.59838283061981,12.917426867210255],[77.59833991527556,12.916904000460743]],"type":"LineString"}
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