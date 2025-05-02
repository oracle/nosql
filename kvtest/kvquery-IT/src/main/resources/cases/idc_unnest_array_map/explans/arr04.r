compiled-query-plan

{
"query file" : "idc_unnest_array_map/q/arr04.q",
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
      "target table" : "User",
      "row variable" : "$u",
      "index used" : "idx_state_areacode_kind",
      "covering index" : true,
      "index row variable" : "$u_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "index filtering predicate" :
      {
        "iterator kind" : "EQUAL",
        "left operand" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "addresses[].phones[][].kind",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$u_idx"
          }
        },
        "right operand" :
        {
          "iterator kind" : "CONST",
          "value" : "work"
        }
      },
      "position in join" : 0
    },
    "FROM variable" : "$u_idx",
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
            "variable" : "$u_idx"
          }
        }
      },
      {
        "field name" : "areacode",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "addresses[].phones[][].areacode",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$u_idx"
          }
        }
      }
    ]
  }
}
}