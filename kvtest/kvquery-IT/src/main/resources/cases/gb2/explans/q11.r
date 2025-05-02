compiled-query-plan

{
"query file" : "gb2/q/q11.q",
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
      "target table" : "T1",
      "row variable" : "$$t",
      "index used" : "idx_mixed_3",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"ANUM":1.7976931348623157E+3082,"AREC.ABOO":true},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "SELECT expressions" : [
      {
        "field name" : "AENU",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "AENU",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "AJSO",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$t"
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}