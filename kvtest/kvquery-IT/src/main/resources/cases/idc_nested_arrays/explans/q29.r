compiled-query-plan

{
"query file" : "idc_nested_arrays/q/q29.q",
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
      "target table" : "nestedTable",
      "row variable" : "$$nt",
      "index used" : "idx_foo_bar",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"maps[].values().foo":"hhh"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$nt",
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
              "iterator kind" : "ANY_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "foo",
                "input iterator" :
                {
                  "iterator kind" : "VALUES",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : "hhh"
              }
            },
            {
              "iterator kind" : "ANY_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "bar",
                "input iterator" :
                {
                  "iterator kind" : "VALUES",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$element"
                  }
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 34
              }
            }
          ]
        },
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "maps",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$nt"
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
            "variable" : "$$nt"
          }
        }
      }
    ]
  }
}
}