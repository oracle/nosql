compiled-query-plan

{
"query file" : "unnest_json/q/arrmap03.q",
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
      "target table" : "Foo",
      "row variable" : "$$t",
      "index used" : "idx_children_values",
      "covering index" : false,
      "index row variable" : "$$t_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.children.values().age":9},
          "range conditions" : { "info.children.values().school" : { "start value" : "a", "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "phones",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "address",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$t"
            }
          }
        }
      }
    },
    "FROM variable" : "$phone",
    "WHERE" : 
    {
      "iterator kind" : "EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "areacode",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$phone"
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 500
      }
    },
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
            "variable" : "$$t_idx"
          }
        }
      }
    ]
  }
}
}