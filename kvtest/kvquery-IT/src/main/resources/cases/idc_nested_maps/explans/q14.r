compiled-query-plan

{
"query file" : "idc_nested_maps/q/q14.q",
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
      "row variable" : "$nt",
      "index used" : "idx_map1_keys_map2_values",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"map1.keys()":"key1"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$nt",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "VALUES",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "map2",
          "input iterator" :
          {
            "iterator kind" : "VALUES",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "map1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$nt"
              }
            }
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 35
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
            "variable" : "$nt"
          }
        }
      }
    ]
  }
}
}