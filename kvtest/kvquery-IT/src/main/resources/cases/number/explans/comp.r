compiled-query-plan

{
"query file" : "number/q/comp.q",
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
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$NumTable_idx"
            }
          },
          "right operand" :
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
        "field name" : "Column_4",
        "field expression" : 
        {
          "iterator kind" : "NOT_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$NumTable_idx"
            }
          },
          "right operand" :
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
          "iterator kind" : "LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$NumTable_idx"
            }
          },
          "right operand" :
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
        "field name" : "Column_6",
        "field expression" : 
        {
          "iterator kind" : "LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$NumTable_idx"
            }
          },
          "right operand" :
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
        "field name" : "Column_7",
        "field expression" : 
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "num",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$NumTable_idx"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 0
          }
        }
      },
      {
        "field name" : "Column_8",
        "field expression" : 
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "CONST",
            "value" : 0
          },
          "right operand" :
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
        "field name" : "Column_9",
        "field expression" : 
        {
          "iterator kind" : "GREATER_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "num",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$NumTable_idx"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 1
          }
        }
      }
    ]
  }
}
}