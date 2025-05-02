compiled-query-plan

{
"query file" : "idc_in_expr/q/q10.q",
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
      "target table" : "ComplexType",
      "row variable" : "$$ComplexType",
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
    "FROM variable" : "$$ComplexType",
    "WHERE" : 
    {
      "iterator kind" : "IN",
      "left-hand-side expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "firstName",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$ComplexType"
          }
        }
      ],
      "right-hand-side expressions" : [
        {
          "iterator kind" : "CONST",
          "value" : ""
        }
      ]
    },
    "SELECT expressions" : [
      {
        "field name" : "ComplexType",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$ComplexType"
        }
      }
    ]
  }
}
}