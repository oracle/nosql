compiled-query-plan

{
"query file" : "geo/q/cmp02.q",
"plan" : 
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
      "target table" : "companies",
      "row variable" : "$$c",
      "index used" : "idx_loc",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q97bp", "start inclusive" : true, "end value" : "9q97bp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q97br", "start inclusive" : true, "end value" : "9q97br", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q97bx", "start inclusive" : true, "end value" : "9q97bx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q97bz", "start inclusive" : true, "end value" : "9q97bz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q97cp", "start inclusive" : true, "end value" : "9q97cp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q97cr", "start inclusive" : true, "end value" : "9q97cr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9k00", "start inclusive" : true, "end value" : "9q9k0nzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9k0q", "start inclusive" : true, "end value" : "9q9k0q", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9k0s", "start inclusive" : true, "end value" : "9q9k0wzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9k0y", "start inclusive" : true, "end value" : "9q9k0y", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9k10", "start inclusive" : true, "end value" : "9q9k17zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9k1h", "start inclusive" : true, "end value" : "9q9k1nzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9k1q", "start inclusive" : true, "end value" : "9q9k1q", "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$c",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "ARRAY_FILTER",
        "predicate iterator" :
        {
          "iterator kind" : "FN_GEO_INSIDE",
          "search target iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$element"
          },
          "search geometry iterator" :
          {
            "iterator kind" : "CONST",
            "value" : {"coordinates":[[[-121.991191,37.27234],[-121.957718,37.262778],[-121.934543,37.286955],[-121.9445,37.302114],[-121.970249,37.294057],[-121.991191,37.27234]]],"type":"polygon"}
          }
        },
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "loc",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "locations",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$c"
            }
          }
        }
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
            "variable" : "$$c"
          }
        }
      }
    ]
  }
}
}