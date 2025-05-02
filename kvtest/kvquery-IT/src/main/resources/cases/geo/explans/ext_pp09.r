compiled-query-plan

{
"query file" : "geo/q/ext_pp09.q",
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
          "range conditions" : { "info.point" : { "start value" : "EMPTY", "start inclusive" : false, "end value" : "EMPTY", "end inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "MAP_CONSTRUCTOR",
          "input iterators" : [
            {
              "iterator kind" : "CONST",
              "value" : "type"
            },
            {
              "iterator kind" : "EXTERNAL_VAR_REF",
              "variable" : "$point"
            },
            {
              "iterator kind" : "CONST",
              "value" : "coordinates"
            },
            {
              "iterator kind" : "ARRAY_CONSTRUCTOR",
              "conditional" : false,
              "input iterators" : [
                {
                  "iterator kind" : "EXTERNAL_VAR_REF",
                  "variable" : "$p2x"
                },
                {
                  "iterator kind" : "CONST",
                  "value" : 35.5156
                }
              ]
            }
          ]
        },
        {
          "iterator kind" : "CONST",
          "value" : 100000.0
        }
      ],
      "map of key bind expressions" : [
        [ 0, 1 ]
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
        "iterator kind" : "MAP_CONSTRUCTOR",
        "input iterators" : [
          {
            "iterator kind" : "CONST",
            "value" : "type"
          },
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$point"
          },
          {
            "iterator kind" : "CONST",
            "value" : "coordinates"
          },
          {
            "iterator kind" : "ARRAY_CONSTRUCTOR",
            "conditional" : false,
            "input iterators" : [
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$p2x"
              },
              {
                "iterator kind" : "CONST",
                "value" : 35.5156
              }
            ]
          }
        ]
      },
      "distance iterator" :
      {
        "iterator kind" : "CONST",
        "value" : 100000.0
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