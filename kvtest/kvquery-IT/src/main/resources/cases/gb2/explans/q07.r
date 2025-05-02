compiled-query-plan

{
"query file" : "gb2/q/q07.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_PARTITIONS",
      "order by fields at positions" : [ 0, 1, 2, 3, 4 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "T1",
          "row variable" : "$$t",
          "index used" : "primary index",
          "covering index" : true,
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
              "field name" : "ID",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
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
        "GROUP BY" : "Grouping by the first 5 expressions in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "gb-0",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "AINT",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          {
            "field name" : "gb-1",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ALON",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          {
            "field name" : "gb-2",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "AFLO",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          {
            "field name" : "gb-3",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ADOU",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          {
            "field name" : "gb-4",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "ANUM",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          {
            "field name" : "aggr-5",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "ANUM",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            }
          },
          {
            "field name" : "aggr-6",
            "field expression" : 
            {
              "iterator kind" : "FN_COUNT_NUMBERS",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "ANUM",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
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
    "GROUP BY" : "Grouping by the first 5 expressions in the SELECT list",
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
        "field name" : "gb-1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "gb-2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "gb-3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "gb-4",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "gb-4",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "aggr-5",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-5",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$from-1"
            }
          }
        }
      },
      {
        "field name" : "aggr-6",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "aggr-6",
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
      "field name" : "AINT",
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
      "field name" : "ALON",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "AFLO",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-2",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "ADOU",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-3",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "ANUM",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "gb-4",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "Column_6",
      "field expression" : 
      {
        "iterator kind" : "MULTIPLY_DIVIDE",
        "operations and operands" : [
          {
            "operation" : "*",
            "operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "aggr-5",
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
              "field name" : "aggr-6",
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
  ]
}
}