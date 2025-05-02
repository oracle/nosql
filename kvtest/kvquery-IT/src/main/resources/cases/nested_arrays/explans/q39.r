compiled-query-plan

{
"query file" : "nested_arrays/q/q39.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "idx_keys_array",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.maps[].keys()":"key7"},
          "range conditions" : { "info.maps[].values().array[][][]" : { "end value" : 7, "end inclusive" : false } }
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
          "iterator kind" : "LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$element"
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 7
          }
        },
        "input iterator" :
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "ARRAY_FILTER",
            "predicate iterator" :
            {
              "iterator kind" : "ANY_EQUAL",
              "left operand" :
              {
                "iterator kind" : "ARRAY_FILTER",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$element"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 15
              }
            },
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "array",
              "input iterator" :
              {
                "iterator kind" : "VALUES",
                "predicate iterator" :
                {
                  "iterator kind" : "EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$key"
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : "key7"
                  }
                },
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "maps",
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