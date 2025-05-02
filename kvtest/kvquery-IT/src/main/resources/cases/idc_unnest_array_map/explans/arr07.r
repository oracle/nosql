compiled-query-plan

{
"query file" : "idc_unnest_array_map/q/arr07.q",
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
      "index used" : "idx_areacode_kind",
      "covering index" : false,
      "index row variable" : "$u_idx",
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "addresses[].phones[][].areacode" : { "end value" : 550, "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$u",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "state",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "addresses",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$u"
          }
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "CA"
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