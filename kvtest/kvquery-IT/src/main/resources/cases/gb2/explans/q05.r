compiled-query-plan

{
"query file" : "gb2/q/q05.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "order by fields at positions" : [ 0 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "T1",
          "row variable" : "$$t",
          "index used" : "idx_BLON",
          "covering index" : false,
          "index row variable" : "$$t_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "index filtering predicate" :
          {
            "iterator kind" : "GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "#ID",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 10023
            }
          },
          "position in join" : 0
        },
        "FROM variable" : "$$t",
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "gb-0",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "BLON",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          {
            "field name" : "aggr-1",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "VALUES",
                "predicate iterator" :
                {
                  "iterator kind" : "NOT_EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$value"
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 0.0
                  }
                },
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "BMAP",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "AREC",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t"
                    }
                  }
                }
              }
            }
          },
          {
            "field name" : "aggr-2",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT_NUMBERS",
              "input iterator" :
              {
                "iterator kind" : "VALUES",
                "predicate iterator" :
                {
                  "iterator kind" : "NOT_EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$value"
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 0.0
                  }
                },
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "BMAP",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "AREC",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t"
                    }
                  }
                }
              }
            }
          },
          {
            "field name" : "aggr-3",
            "field expression" : 
            {
              "iterator kind" : "FUNC_COUNT_STAR"
            }
          }
        ],
        "LIMIT" :
        {
          "iterator kind" : "ADD_SUBTRACT",
          "operations and operands" : [
            {
              "operation" : "+",
              "operand" :
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            },
            {
              "operation" : "+",
              "operand" :
              {
                "iterator kind" : "CONST",
                "value" : 10
              }
            }
          ]
        }
      }
    },
    "FROM variable" : "$from-1",
    "GROUP BY" : "Grouping by the first expression in the SELECT list",
    "SELECT expressions" : [
      {
        "field name" : "gb-0",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-0",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "aggr-1",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-2",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-3",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      }
    ],
    "OFFSET" :
    {
      "iterator kind" : "CONST",
      "value" : 2
    },
    "LIMIT" :
    {
      "iterator kind" : "CONST",
      "value" : 10
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "BLON",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-0",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "Column_2",
      "field expression" : 
      {
        "iterator kind" : "MULTIPLY_DIVIDE",
        "operations and operands" : [
          {
            "operation" : "*",
            "operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$from-0"
              }
            }
          },
          {
            "operation" : "div",
            "operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$from-0"
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
        "iterator kind" : "FIELD_STEP",
        "field name" : "aggr-1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "Column_4",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "aggr-3",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ]
}
}