compiled-query-plan

{
"query file" : "insert/q/ins03r.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "str" : "gtm",
  "id1" : 1,
  "id2" : 100,
  "info" : {
    "a" : 10,
    "b" : 30
  },
  "rec1" : null
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$f",
  "SELECT expressions" : [
    {
      "field name" : "id1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$f"
        }
      }
    },
    {
      "field name" : "id2",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id2",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$f"
        }
      }
    },
    {
      "field name" : "Column_3",
      "field expression" : 
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "LESS_OR_EQUAL",
            "left operand" :
            {
              "iterator kind" : "CONST",
              "value" : 120
            },
            "right operand" :
            {
              "iterator kind" : "FUNC_REMAINING_HOURS",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "FUNC_REMAINING_HOURS",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 144
            }
          }
        ]
      }
    },
    {
      "field name" : "row_size",
      "field expression" : 
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "LESS_OR_EQUAL",
            "left operand" :
            {
              "iterator kind" : "CONST",
              "value" : 135
            },
            "right operand" :
            {
              "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "iterator kind" : "LESS_OR_EQUAL",
            "left operand" :
            {
              "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 145
            }
          }
        ]
      }
    }
  ]
}
}