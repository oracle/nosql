compiled-query-plan

{
"query file" : "nested_arrays/q/tmp08.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "ARRAY_FILTER",
        "predicate iterator" :
        {
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "LESS_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "CONST",
                "value" : 3
              },
              "right operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "a",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$element"
                }
              }
            },
            {
              "iterator kind" : "LESS_OR_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "a",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$element"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 4
              }
            }
          ]
        },
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "array1",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
            }
          }
        }
      }
    },
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}