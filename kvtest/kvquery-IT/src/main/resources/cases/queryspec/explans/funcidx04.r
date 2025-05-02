compiled-query-plan

{
"query file" : "queryspec/q/funcidx04.q",
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
      "target table" : "Users",
      "row variable" : "$$u",
      "index used" : "idx_substring",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"substring#otherNames[].first@,1,2":"re"},
          "range conditions" : { "length#otherNames[].last" : { "start value" : 3, "start inclusive" : false } }
        },
        {
          "equality conditions" : {"substring#otherNames[].first@,1,2":"ir"},
          "range conditions" : { "length#otherNames[].last" : { "start value" : 3, "start inclusive" : false } }
        },
        {
          "equality conditions" : {"substring#otherNames[].first@,1,2":"ai"},
          "range conditions" : { "length#otherNames[].last" : { "start value" : 3, "start inclusive" : false } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$u",
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
            "variable" : "$$u"
          }
        }
      },
      {
        "field name" : "first",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "first",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "otherNames",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$u"
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