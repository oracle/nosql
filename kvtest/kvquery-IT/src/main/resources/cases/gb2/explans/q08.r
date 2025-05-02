compiled-query-plan

{
"query file" : "gb2/q/q08.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0, 1 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "T1",
        "row variable" : "$$t",
        "index used" : "idx_mixed_1",
        "covering index" : true,
        "index row variable" : "$$t_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$t_idx",
      "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "ALON",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ALON",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t_idx"
            }
          }
        },
        {
          "field name" : "ADOU",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "AREC.ADOU",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t_idx"
            }
          }
        },
        {
          "field name" : "Column_3",
          "field expression" : 
          {
            "iterator kind" : "FUNC_SUM",
            "input iterator" :
            {
              "iterator kind" : "ADD_SUBTRACT",
              "operations and operands" : [
                {
                  "operation" : "+",
                  "operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "#ADOU",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t_idx"
                    }
                  }
                },
                {
                  "operation" : "+",
                  "operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "ALON",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$$t_idx"
                    }
                  }
                }
              ]
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
  "SELECT expressions" : [
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
      "field name" : "Column_3",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "Column_3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ]
}
}