compiled-query-plan

{
"query file" : "idc_in_expr/q/q1.q",
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
      "row variable" : "$$SimpleDatatype",
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
    "FROM variable" : "$$SimpleDatatype",
    "WHERE" : 
    {
      "iterator kind" : "IN",
      "left-hand-side expressions" : [
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "name",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$SimpleDatatype"
          }
        }
      ],
      "right-hand-side expressions" : [
        {
          "iterator kind" : "CONST",
          "value" : "Ram"
        },
        {
          "iterator kind" : "CONST",
          "value" : "Rohit"
        },
        {
          "iterator kind" : "CONST",
          "value" : null
        }
      ]
    },
    "SELECT expressions" : [
      {
        "field name" : "SimpleDatatype",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$SimpleDatatype"
        }
      }
    ]
  }
}
}