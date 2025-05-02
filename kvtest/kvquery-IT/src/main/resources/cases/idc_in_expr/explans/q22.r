compiled-query-plan

{
"query file" : "idc_in_expr/q/q22.q",
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
          "iterator kind" : "CONST",
          "value" : true
        }
      ],
      "right-hand-side expressions" : [
        {
          "iterator kind" : "IS_OF_TYPE",
          "target types" : [
            {
            "type" : "Json",
            "quantifier" : "",
            "only" : false
            }
          ],
          "input iterator" :
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
    },
    "SELECT expressions" : [
      {
        "field name" : "simpleDatatype",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$simpleDatatype"
        }
      }
    ]
  }
}
}