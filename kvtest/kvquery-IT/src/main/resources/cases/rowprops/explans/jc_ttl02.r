compiled-query-plan

{
"query file" : "rowprops/q/jc_ttl02.q",
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
      "target table" : "Boo",
      "row variable" : "$f",
      "index used" : "idx_state_city_age",
      "covering index" : true,
      "index row variable" : "$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"address.state":"MA"},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "GREATER_THAN",
        "left operand" :
        {
          "iterator kind" : "MULTIPLY_DIVIDE",
          "operations and operands" : [
            {
              "operation" : "*",
              "operand" :
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            },
            {
              "operation" : "*",
              "operand" :
              {
                "iterator kind" : "FUNC_REMAINING_DAYS",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$f_idx"
                }
              }
            }
          ]
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : 3
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$f_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f_idx"
          }
        }
      },
      {
        "field name" : "days",
        "field expression" : 
        {
          "iterator kind" : "MULTIPLY_DIVIDE",
          "operations and operands" : [
            {
              "operation" : "*",
              "operand" :
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            },
            {
              "operation" : "*",
              "operand" :
              {
                "iterator kind" : "FUNC_REMAINING_DAYS",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$f_idx"
                }
              }
            }
          ]
        }
      },
      {
        "field name" : "hours",
        "field expression" : 
        {
          "iterator kind" : "LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "FUNC_REMAINING_HOURS",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$f_idx"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 15
          }
        }
      }
    ]
  }
}
}