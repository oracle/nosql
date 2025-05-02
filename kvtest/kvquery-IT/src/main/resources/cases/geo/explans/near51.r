compiled-query-plan

{
"query file" : "geo/q/near51.q",
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
            "range conditions" : { "info.geom" : { "start value" : "sw2c9", "start inclusive" : true, "end value" : "sw2c9", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw2cc", "start inclusive" : true, "end value" : "sw2cgzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw2cs", "start inclusive" : true, "end value" : "sw2czzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw2f1", "start inclusive" : true, "end value" : "sw2f1", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw2f3", "start inclusive" : true, "end value" : "sw2f7zzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw2fh", "start inclusive" : true, "end value" : "sw2frzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw318", "start inclusive" : true, "end value" : "sw31gzzzzz", "end inclusive" : true } }
          },
          {
            "equality conditions" : {},
            "range conditions" : { "info.geom" : { "start value" : "sw340", "start inclusive" : true, "end value" : "sw347zzzzz", "end inclusive" : true } }
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
            "equality conditions" : {"info.geom":"sw2"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw2c"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw2f"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw31"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw3"},
            "range conditions" : {}
          },
          {
            "equality conditions" : {"info.geom":"sw34"},
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
          "value" : {"coordinates":[[24.0254,35.4849],[23.977,35.507],[23.7676,35.5347],[23.6536,35.49]],"type":"linestring"}
        },
        "distance iterator" :
        {
          "iterator kind" : "CONST",
          "value" : 5000.0
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
              "value" : {"coordinates":[[24.0254,35.4849],[23.977,35.507],[23.7676,35.5347],[23.6536,35.49]],"type":"linestring"}
            }
          }
        }
      ]
    }
  }
}
}