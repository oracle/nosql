compiled-query-plan

{
"query file" : "idc_in_expr/q/q25.q",
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
      "target table" : "SimpleDatatype",
      "row variable" : "$$simpleDatatype",
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
    "FROM variable" : "$$simpleDatatype",
    "WHERE" : 
    {
      "iterator kind" : "IN",
      "left-hand-side expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rank",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$simpleDatatype"
          }
        }
      ],
      "right-hand-side expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$k1"
        }
      ]
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
            "variable" : "$$simpleDatatype"
          }
        }
      }
    ]
  }
}
}