compiled-query-plan

{
"query file" : "gb2/q/q03.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "order by fields at positions" : [ 0, 1, 2, 3 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "T1",
        "row variable" : "$$t",
        "index used" : "primary index",
        "covering index" : false,
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
      "GROUP BY" : "Grouping by the first 4 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "AINT",
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
          "field name" : "ALON",
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
          "field name" : "AFLO",
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
          "field name" : "ADOU",
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
          "field name" : "cnt",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        },
        {
          "field name" : "sum",
          "field expression" : 
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "VALUES",
              "predicate iterator" :
              {
                "iterator kind" : "GREATER_THAN",
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
  "GROUP BY" : "Grouping by the first 4 expressions in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "AINT",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "AINT",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "ALON",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "ALON",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "AFLO",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "AFLO",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "ADOU",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "ADOU",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "cnt",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "cnt",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    },
    {
      "field name" : "sum",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "sum",
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
}
}