compiled-query-plan

{
"query file" : "insert/q/ins08r.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "str" : null,
  "id1" : 1,
  "id2" : 208,
  "info" : null,
  "rec1" : {
    "long" : 5,
    "rec2" : {
      "str" : null,
      "num" : 2.3E+3,
      "arr" : [10, 4, 6]
    },
    "recinfo" : null,
    "map" : {
      "bar" : "dff",
      "foo" : "xyz"
    }
  }
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$$f",
  "SELECT expressions" : [
    {
      "field name" : "str",
      "field expression" : 
      {
        "iterator kind" : "OP_IS_NULL",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "str",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      }
    },
    {
      "field name" : "info",
      "field expression" : 
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : null
        }
      }
    },
    {
      "field name" : "str2",
      "field expression" : 
      {
        "iterator kind" : "OP_IS_NULL",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "str",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "rec2",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "rec1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$f"
              }
            }
          }
        }
      }
    },
    {
      "field name" : "Column_4",
      "field expression" : 
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "recinfo",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "rec1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : null
        }
      }
    }
  ]
}
}