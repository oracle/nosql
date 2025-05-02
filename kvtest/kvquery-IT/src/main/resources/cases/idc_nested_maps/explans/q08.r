compiled-query-plan

{
"query file" : "idc_nested_maps/q/q08.q",
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
      "index used" : "idx_age_areacode_kind",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"age":17},
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
        "iterator kind" : "KEYS",
        "input iterator" :
        {
          "iterator kind" : "VALUES",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "phones",
            "input iterator" :
            {
              "iterator kind" : "VALUES",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "addresses",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$nt"
                }
              }
            }
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "phone1"
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