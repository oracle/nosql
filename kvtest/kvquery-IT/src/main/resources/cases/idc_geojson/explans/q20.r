compiled-query-plan

{
"query file" : "idc_geojson/q/q20.q",
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
          "range conditions" : { "info.point" : { "start value" : "ttt98", "start inclusive" : true, "end value" : "ttt9gzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttt9s", "start inclusive" : true, "end value" : "ttt9zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttc8", "start inclusive" : true, "end value" : "tttcgzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttcs", "start inclusive" : true, "end value" : "tttd9zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttdd", "start inclusive" : true, "end value" : "tttdezzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttdh", "start inclusive" : true, "end value" : "tttdtzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttdw", "start inclusive" : true, "end value" : "tttdxzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttf0", "start inclusive" : true, "end value" : "tttf9zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttfd", "start inclusive" : true, "end value" : "tttfezzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttfh", "start inclusive" : true, "end value" : "tttftzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tttfw", "start inclusive" : true, "end value" : "tttfxzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttw18", "start inclusive" : true, "end value" : "ttw1gzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttw1s", "start inclusive" : true, "end value" : "ttw1s", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttw1u", "start inclusive" : true, "end value" : "ttw1u", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttw40", "start inclusive" : true, "end value" : "ttw49zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttw4d", "start inclusive" : true, "end value" : "ttw4ezzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttw4h", "start inclusive" : true, "end value" : "ttw4h", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttw4k", "start inclusive" : true, "end value" : "ttw4k", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "ttw4s", "start inclusive" : true, "end value" : "ttw4s", "end inclusive" : true } }
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
        "value" : {"coordinates":[[[75.25634765625,31.203404950917395],[76.1187744140625,31.203404950917395],[76.1187744140625,31.39115752282472],[75.25634765625,31.39115752282472],[75.25634765625,31.203404950917395]]],"type":"Polygon"}
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