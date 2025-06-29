compiled-query-plan

{
"query file" : "number/q/isoftype.q",
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
      "target table" : "NumTable",
      "row variable" : "$$NumTable",
      "index used" : "idx_num1",
      "covering index" : true,
      "index row variable" : "$$NumTable_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$NumTable_idx",
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
            "variable" : "$$NumTable_idx"
          }
        }
      },
      {
        "field name" : "num",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "num",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$NumTable_idx"
          }
        }
      },
      {
        "field name" : "Column_3",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Number",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$NumTable_idx"
            }
          }
        }
      },
      {
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Number",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "num",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$NumTable_idx"
            }
          }
        }
      },
      {
        "field name" : "Column_5",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Number",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "ADD_SUBTRACT",
            "operations and operands" : [
              {
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#id",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$NumTable_idx"
                  }
                }
              },
              {
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "num",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$NumTable_idx"
                  }
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_6",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Number",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "MULTIPLY_DIVIDE",
            "operations and operands" : [
              {
                "operation" : "*",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#id",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$NumTable_idx"
                  }
                }
              },
              {
                "operation" : "*",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "num",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$NumTable_idx"
                  }
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_7",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Number",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "ADD_SUBTRACT",
            "operations and operands" : [
              {
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "#id",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$NumTable_idx"
                  }
                }
              },
              {
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_8",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Number",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "MULTIPLY_DIVIDE",
            "operations and operands" : [
              {
                "operation" : "*",
                "operand" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "num",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$NumTable_idx"
                  }
                }
              },
              {
                "operation" : "/",
                "operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 2
                }
              }
            ]
          }
        }
      },
      {
        "field name" : "Column_9",
        "field expression" : 
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Integer",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
          {
            "iterator kind" : "CAST",
            "target type" : "Integer",
            "quantifier" : "",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "num",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$NumTable_idx"
              }
            }
          }
        }
      }
    ]
  }
}
}