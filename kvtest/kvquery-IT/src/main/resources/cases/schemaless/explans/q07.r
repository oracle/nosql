compiled-query-plan

{
"query file" : "schemaless/q/q07.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Viewers",
      "row variable" : "$$v",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$v",
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "shows",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$v"
        }
      }
    },
    "FROM variable" : "$show",
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "seasons",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$show"
        }
      }
    },
    "FROM variable" : "$season",
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "episodes",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$season"
        }
      }
    },
    "FROM variable" : "$episode",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "country",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$v"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "USA"
          }
        },
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "showId",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$show"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 16
          }
        },
        {
          "iterator kind" : "ANY_GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "date",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "episodes",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "seasons",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$show"
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : "2021-04-01"
          }
        }
      ]
    },
    "SELECT expressions" : [
      {
        "field name" : "acct_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "acct_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v"
          }
        }
      },
      {
        "field name" : "user_id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "user_id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$v"
          }
        }
      },
      {
        "field name" : "showName",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "showName",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$show"
              }
            }
          ]
        }
      },
      {
        "field name" : "seasonNum",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "seasonNum",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$season"
              }
            }
          ]
        }
      },
      {
        "field name" : "episodeID",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "episodeID",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$episode"
              }
            }
          ]
        }
      },
      {
        "field name" : "date",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "date",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$episode"
              }
            }
          ]
        }
      }
    ]
  }
}
}