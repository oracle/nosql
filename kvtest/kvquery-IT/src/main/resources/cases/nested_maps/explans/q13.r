compiled-query-plan

{
"query file" : "nested_maps/q/q13.q",
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
      "index used" : "idx_keys_areacode_number",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.addresses[].phones[].values().keys()":"phone1","info.addresses[].phones[].values().values().areacode":408},
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
        "iterator kind" : "VALUES",
        "predicate iterator" :
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "number",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$value"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 30
          }
        },
        "input iterator" :
        {
          "iterator kind" : "VALUES",
          "predicate iterator" :
          {
            "iterator kind" : "ANY_EQUAL",
            "left operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "areacode",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "phone1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$value"
                }
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 408
            }
          },
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "phones",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "addresses",
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