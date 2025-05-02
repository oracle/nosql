compiled-query-plan

{
"query file" : "geo/q/cmp01.q",
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
          "range conditions" : { "locations[].loc" : { "start value" : "9q9her", "start inclusive" : true, "end value" : "9q9her", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hex", "start inclusive" : true, "end value" : "9q9hex", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hez", "start inclusive" : true, "end value" : "9q9hez", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hg2", "start inclusive" : true, "end value" : "9q9hg3zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hg6", "start inclusive" : true, "end value" : "9q9hggzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hgk", "start inclusive" : true, "end value" : "9q9hgmzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hgq", "start inclusive" : true, "end value" : "9q9hgzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hsp", "start inclusive" : true, "end value" : "9q9hsp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hsr", "start inclusive" : true, "end value" : "9q9hsr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hsx", "start inclusive" : true, "end value" : "9q9hsx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hsz", "start inclusive" : true, "end value" : "9q9hsz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9htp", "start inclusive" : true, "end value" : "9q9htp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9htr", "start inclusive" : true, "end value" : "9q9htr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hu0", "start inclusive" : true, "end value" : "9q9hv7zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9hvh", "start inclusive" : true, "end value" : "9q9hvrzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9j52", "start inclusive" : true, "end value" : "9q9j53zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9j56", "start inclusive" : true, "end value" : "9q9j56", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9j58", "start inclusive" : true, "end value" : "9q9j5dzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9j5f", "start inclusive" : true, "end value" : "9q9j5f", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9jh0", "start inclusive" : true, "end value" : "9q9jh4zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9jh6", "start inclusive" : true, "end value" : "9q9jh6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9jh8", "start inclusive" : true, "end value" : "9q9jhdzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9jhf", "start inclusive" : true, "end value" : "9q9jhf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9jj0", "start inclusive" : true, "end value" : "9q9jj4zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "locations[].loc" : { "start value" : "9q9jj6", "start inclusive" : true, "end value" : "9q9jj6", "end inclusive" : true } }
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
            "value" : {"coordinates":[[[-122.194164,37.431279],[-122.13168,37.396102],[-122.104214,37.428825],[-122.146443,37.456492],[-122.194164,37.431279]]],"type":"polygon"}
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
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "ARRAY_FILTER",
              "predicate iterator" :
              {
                "iterator kind" : "FN_GEO_INSIDE",
                "search target iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "loc",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                },
                "search geometry iterator" :
                {
                  "iterator kind" : "CONST",
                  "value" : {"coordinates":[[[-122.194164,37.431279],[-122.13168,37.396102],[-122.104214,37.428825],[-122.146443,37.456492],[-122.194164,37.431279]]],"type":"polygon"}
                }
              },
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
          ]
        }
      }
    ]
  }
}
}