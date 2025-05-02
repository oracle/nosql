compiled-query-plan

{
"query file" : "idc_geojson/q/q22.q",
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
      "target table" : "points",
      "row variable" : "$$p",
      "index used" : "idx_ptn",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr15x", "start inclusive" : true, "end value" : "tdr15x", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr15z", "start inclusive" : true, "end value" : "tdr15z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr178", "start inclusive" : true, "end value" : "tdr17gzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr17s", "start inclusive" : true, "end value" : "tdr17zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1dh", "start inclusive" : true, "end value" : "tdr1dzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1e8", "start inclusive" : true, "end value" : "tdr1gzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1hp", "start inclusive" : true, "end value" : "tdr1hp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1hr", "start inclusive" : true, "end value" : "tdr1hr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1hx", "start inclusive" : true, "end value" : "tdr1hx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1hz", "start inclusive" : true, "end value" : "tdr1hz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1jp", "start inclusive" : true, "end value" : "tdr1jp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1jr", "start inclusive" : true, "end value" : "tdr1jr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1jx", "start inclusive" : true, "end value" : "tdr1jx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1jz", "start inclusive" : true, "end value" : "tdr1mzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1np", "start inclusive" : true, "end value" : "tdr1np", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1nr", "start inclusive" : true, "end value" : "tdr1nr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1nx", "start inclusive" : true, "end value" : "tdr1nx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1nz", "start inclusive" : true, "end value" : "tdr1nz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1pp", "start inclusive" : true, "end value" : "tdr1pp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1pr", "start inclusive" : true, "end value" : "tdr1pr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1px", "start inclusive" : true, "end value" : "tdr1px", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr1pz", "start inclusive" : true, "end value" : "tdr1zzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr30p", "start inclusive" : true, "end value" : "tdr30p", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr320", "start inclusive" : true, "end value" : "tdr321zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr324", "start inclusive" : true, "end value" : "tdr325zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr32h", "start inclusive" : true, "end value" : "tdr32jzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr32n", "start inclusive" : true, "end value" : "tdr32pzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr380", "start inclusive" : true, "end value" : "tdr381zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr384", "start inclusive" : true, "end value" : "tdr385zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr38h", "start inclusive" : true, "end value" : "tdr38jzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr38n", "start inclusive" : true, "end value" : "tdr38pzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr3b0", "start inclusive" : true, "end value" : "tdr3b1zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr3b4", "start inclusive" : true, "end value" : "tdr3b5zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr3bh", "start inclusive" : true, "end value" : "tdr3bjzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr3bn", "start inclusive" : true, "end value" : "tdr3bpzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr440", "start inclusive" : true, "end value" : "tdr464zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr466", "start inclusive" : true, "end value" : "tdr466", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr468", "start inclusive" : true, "end value" : "tdr46dzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr46f", "start inclusive" : true, "end value" : "tdr46f", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr470", "start inclusive" : true, "end value" : "tdr474zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr476", "start inclusive" : true, "end value" : "tdr476", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr478", "start inclusive" : true, "end value" : "tdr47dzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr47f", "start inclusive" : true, "end value" : "tdr47f", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4h0", "start inclusive" : true, "end value" : "tdr4jhzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4jk", "start inclusive" : true, "end value" : "tdr4jk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4js", "start inclusive" : true, "end value" : "tdr4js", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4ju", "start inclusive" : true, "end value" : "tdr4ju", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4k0", "start inclusive" : true, "end value" : "tdr4k4zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4k6", "start inclusive" : true, "end value" : "tdr4k6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4k8", "start inclusive" : true, "end value" : "tdr4kdzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4kf", "start inclusive" : true, "end value" : "tdr4kf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4n0", "start inclusive" : true, "end value" : "tdr4nhzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4nk", "start inclusive" : true, "end value" : "tdr4nk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4ns", "start inclusive" : true, "end value" : "tdr4ns", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4nu", "start inclusive" : true, "end value" : "tdr4nu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4p0", "start inclusive" : true, "end value" : "tdr4phzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4pk", "start inclusive" : true, "end value" : "tdr4pk", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4ps", "start inclusive" : true, "end value" : "tdr4ps", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr4pu", "start inclusive" : true, "end value" : "tdr4pu", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr600", "start inclusive" : true, "end value" : "tdr601zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr604", "start inclusive" : true, "end value" : "tdr605zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "tdr60h", "start inclusive" : true, "end value" : "tdr60h", "end inclusive" : true } }
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$p",
    "WHERE" : 
    {
      "iterator kind" : "FN_GEO_INTERSECT",
      "search target iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "point",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "info",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$p"
          }
        }
      },
      "search geometry iterator" :
      {
        "iterator kind" : "CONST",
        "value" : {"coordinates":[[[[77.50614166259766,12.966754416691947],[77.60295867919922,12.947348706524428],[77.61051177978516,12.908198108318507],[77.50511169433594,12.888787845039232],[77.55146026611328,12.879416834404825],[77.59712219238281,12.875735270042483],[77.65617370605469,12.901170428290447],[77.70492553710938,12.963743284940753],[77.67539978027344,13.033659039547265],[77.52056121826172,12.999874459369373],[77.50614166259766,12.966754416691947]]],[[[77.55935668945312,13.057740282292977],[77.52948760986328,13.065766841807562],[77.45155334472656,13.048375633247892],[77.4481201171875,13.016600074665284],[77.43438720703125,12.94701411205675],[77.46047973632811,12.962739566261234],[77.49000549316406,12.967758119178917],[77.50991821289062,12.982144076424275],[77.49378204345703,13.004892262383622],[77.52124786376953,13.032655603554861],[77.55935668945312,13.056402497014313],[77.52777099609375,13.002550633604786],[77.49412536621094,13.00355419150191],[77.55935668945312,13.057740282292977]]]],"type":"MultiPolygon"}
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
            "variable" : "$$p"
          }
        }
      },
      {
        "field name" : "point",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "point",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "info",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$p"
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