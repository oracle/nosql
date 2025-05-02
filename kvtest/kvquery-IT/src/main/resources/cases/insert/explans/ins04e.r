compiled-query-plan

{
"query file" : "insert/q/ins04e.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "id1" : 1,
  "info" : null,
  "rec1" : {
    "long" : 120,
    "rec2" : {
      "str" : "dfg",
      "num" : 1E+1,
      "arr" : [10, 4, 6]
    },
    "recinfo" : {
      "a" : 4,
      "b" : "str"
    },
    "map" : {
      "bar" : "dff",
      "foo" : "xyz"
    }
  }
},
    "column positions" : [ 0, 2 ],
    "value iterators" : [
      {
        "iterator kind" : "CAST",
        "target type" : "String",
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$f1"
        }
      },
      {
        "iterator kind" : "CAST",
        "target type" : "Integer",
        "quantifier" : "?",
        "input iterator" :
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$id2"
        }
      }
    ],
    "TTL iterator" :
    {
      "iterator kind" : "CONST",
      "value" : 6
    }
  },
  "FROM variable" : "$f",
  "SELECT expressions" : [
    {
      "field name" : "Column_1",
      "field expression" : 
      {
        "iterator kind" : "FUNC_REMAINING_DAYS",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$f"
        }
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
              "value" : 148
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
              "value" : 158
            }
          }
        ]
      }
    }
  ]
}
}