compiled-query-plan

{
"query file" : "json_idx/q/q13.q",
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
      "target table" : "Bar",
      "row variable" : "$$tb",
      "index used" : "idx_product_agreementIdTag",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.agreement[].agreementId":"8455100643929731","info.tag":"Live"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$tb",
    "SELECT expressions" : [
      {
        "field name" : "agreementId",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "agreementId",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "agreement",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "info",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$tb"
                  }
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