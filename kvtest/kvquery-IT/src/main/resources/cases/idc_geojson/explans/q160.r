compiled-query-plan

{
"query file" : "idc_geojson/q/q160.q",
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
      "index used" : "idx_kind_ptn",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdqbz", "start inclusive" : true, "end value" : "tdqbz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdqcp", "start inclusive" : true, "end value" : "tdqcp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdqcr", "start inclusive" : true, "end value" : "tdqcr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdqcx", "start inclusive" : true, "end value" : "tdqcx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdqcz", "start inclusive" : true, "end value" : "tdqcz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdqfp", "start inclusive" : true, "end value" : "tdqfp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdqfr", "start inclusive" : true, "end value" : "tdqfr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdqfx", "start inclusive" : true, "end value" : "tdqfx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr0b", "start inclusive" : true, "end value" : "tdr0czzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr0f", "start inclusive" : true, "end value" : "tdr0gzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr0u", "start inclusive" : true, "end value" : "tdr0vzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr0y", "start inclusive" : true, "end value" : "tdr1zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr2b", "start inclusive" : true, "end value" : "tdr2b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr30", "start inclusive" : true, "end value" : "tdr30", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr32", "start inclusive" : true, "end value" : "tdr32", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr38", "start inclusive" : true, "end value" : "tdr38", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr3b", "start inclusive" : true, "end value" : "tdr3b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr40", "start inclusive" : true, "end value" : "tdr49zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr4d", "start inclusive" : true, "end value" : "tdr4ezzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr4h", "start inclusive" : true, "end value" : "tdr4tzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr4w", "start inclusive" : true, "end value" : "tdr4xzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr60", "start inclusive" : true, "end value" : "tdr60", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr62", "start inclusive" : true, "end value" : "tdr62", "end inclusive" : true } }
        },
        {
          "equality conditions" : {"info.kind":"hotel"},
          "range conditions" : { "info.point" : { "start value" : "tdr68", "start inclusive" : true, "end value" : "tdr68", "end inclusive" : true } }
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
        "value" : {"coordinates":[[[77.3935317993164,13.062088034413131],[77.33207702636719,12.99050762232266],[77.38082885742188,12.85464890558895],[77.52914428710938,12.8171576642436],[77.65205383300781,12.828539524713626],[77.71041870117188,12.912883118595694],[77.72621154785156,12.991845763535508],[77.69462585449217,13.109239500475589],[77.55180358886719,13.112248862097216],[77.53978729248047,13.078475027303307],[77.3935317993164,13.062088034413131]]],"type":"Polygon"}
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