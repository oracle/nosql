compiled-query-plan

{
"query file" : "geo/q/int16.q",
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
          "range conditions" : { "info.point" : { "start value" : "sw2b8bs", "start inclusive" : true, "end value" : "sw2b8bzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b8ch", "start inclusive" : true, "end value" : "sw2b8czzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b8fh", "start inclusive" : true, "end value" : "sw2b8fzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b8gh", "start inclusive" : true, "end value" : "sw2b8gzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b8uh", "start inclusive" : true, "end value" : "sw2b8uzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b8vh", "start inclusive" : true, "end value" : "sw2b8vrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b908", "start inclusive" : true, "end value" : "sw2b90gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b90s", "start inclusive" : true, "end value" : "sw2b91zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b928", "start inclusive" : true, "end value" : "sw2b92gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b92s", "start inclusive" : true, "end value" : "sw2b97zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b988", "start inclusive" : true, "end value" : "sw2b98gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b98s", "start inclusive" : true, "end value" : "sw2b99zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9b8", "start inclusive" : true, "end value" : "sw2b9bgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9bs", "start inclusive" : true, "end value" : "sw2b9j7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9jh", "start inclusive" : true, "end value" : "sw2b9jrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9k0", "start inclusive" : true, "end value" : "sw2b9m7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9mh", "start inclusive" : true, "end value" : "sw2b9mrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9s0", "start inclusive" : true, "end value" : "sw2b9t7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9th", "start inclusive" : true, "end value" : "sw2b9trzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9u0", "start inclusive" : true, "end value" : "sw2b9v7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2b9vh", "start inclusive" : true, "end value" : "sw2b9vrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd08", "start inclusive" : true, "end value" : "sw2bd0gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd0s", "start inclusive" : true, "end value" : "sw2bd0s", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd0u", "start inclusive" : true, "end value" : "sw2bd0u", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd10", "start inclusive" : true, "end value" : "sw2bd1hzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd1k", "start inclusive" : true, "end value" : "sw2bd1k", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd1s", "start inclusive" : true, "end value" : "sw2bd1s", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd1u", "start inclusive" : true, "end value" : "sw2bd1u", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd40", "start inclusive" : true, "end value" : "sw2bd4hzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd4k", "start inclusive" : true, "end value" : "sw2bd4k", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd4s", "start inclusive" : true, "end value" : "sw2bd4s", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd4u", "start inclusive" : true, "end value" : "sw2bd4u", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd50", "start inclusive" : true, "end value" : "sw2bd5zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bd70", "start inclusive" : true, "end value" : "sw2bd7zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bde0", "start inclusive" : true, "end value" : "sw2bdezzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bdg0", "start inclusive" : true, "end value" : "sw2bdj7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bdjh", "start inclusive" : true, "end value" : "sw2bdmzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bdnh", "start inclusive" : true, "end value" : "sw2bdnzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bdph", "start inclusive" : true, "end value" : "sw2bdzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2be50", "start inclusive" : true, "end value" : "sw2be5zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2be70", "start inclusive" : true, "end value" : "sw2be7gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2beh0", "start inclusive" : true, "end value" : "sw2bekgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bem0", "start inclusive" : true, "end value" : "sw2bemgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bemk", "start inclusive" : true, "end value" : "sw2bemmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bemq", "start inclusive" : true, "end value" : "sw2berzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bet2", "start inclusive" : true, "end value" : "sw2bet3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bet6", "start inclusive" : true, "end value" : "sw2betgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2betk", "start inclusive" : true, "end value" : "sw2betmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2betq", "start inclusive" : true, "end value" : "sw2betzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bev2", "start inclusive" : true, "end value" : "sw2bev3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bev6", "start inclusive" : true, "end value" : "sw2bevgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bevk", "start inclusive" : true, "end value" : "sw2bevmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bevq", "start inclusive" : true, "end value" : "sw2bezzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg25", "start inclusive" : true, "end value" : "sw2bg25", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg27", "start inclusive" : true, "end value" : "sw2bg27", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg2e", "start inclusive" : true, "end value" : "sw2bg2e", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg2g", "start inclusive" : true, "end value" : "sw2bg2zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg35", "start inclusive" : true, "end value" : "sw2bg35", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg37", "start inclusive" : true, "end value" : "sw2bg37", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg3e", "start inclusive" : true, "end value" : "sw2bg3e", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg3g", "start inclusive" : true, "end value" : "sw2bg3zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg65", "start inclusive" : true, "end value" : "sw2bg65", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg6h", "start inclusive" : true, "end value" : "sw2bg6jzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg6n", "start inclusive" : true, "end value" : "sw2bg6pzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bg80", "start inclusive" : true, "end value" : "sw2bgd1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bgd4", "start inclusive" : true, "end value" : "sw2bgd5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bgdh", "start inclusive" : true, "end value" : "sw2bgdjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bgdn", "start inclusive" : true, "end value" : "sw2bgdpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bgf0", "start inclusive" : true, "end value" : "sw2bgf1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bgf4", "start inclusive" : true, "end value" : "sw2bgf5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bgfh", "start inclusive" : true, "end value" : "sw2bgfjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bgfn", "start inclusive" : true, "end value" : "sw2bgfpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsj2", "start inclusive" : true, "end value" : "sw2bsj3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsj6", "start inclusive" : true, "end value" : "sw2bsjgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsjk", "start inclusive" : true, "end value" : "sw2bsjmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsjq", "start inclusive" : true, "end value" : "sw2bsjzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsm2", "start inclusive" : true, "end value" : "sw2bsm3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsm6", "start inclusive" : true, "end value" : "sw2bsmgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsmk", "start inclusive" : true, "end value" : "sw2bsmmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsmq", "start inclusive" : true, "end value" : "sw2bsrzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bst2", "start inclusive" : true, "end value" : "sw2bst3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bst6", "start inclusive" : true, "end value" : "sw2bst6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bst8", "start inclusive" : true, "end value" : "sw2bstdzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bstf", "start inclusive" : true, "end value" : "sw2bstf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsw0", "start inclusive" : true, "end value" : "sw2bsw4zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsw6", "start inclusive" : true, "end value" : "sw2bsw6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsw8", "start inclusive" : true, "end value" : "sw2bswdzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bswf", "start inclusive" : true, "end value" : "sw2bswf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsx0", "start inclusive" : true, "end value" : "sw2bsx4zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsx6", "start inclusive" : true, "end value" : "sw2bsx6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsx8", "start inclusive" : true, "end value" : "sw2bsxdzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsxf", "start inclusive" : true, "end value" : "sw2bsxgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsxu", "start inclusive" : true, "end value" : "sw2bsxvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bsxy", "start inclusive" : true, "end value" : "sw2bsxzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bszb", "start inclusive" : true, "end value" : "sw2bszczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bszf", "start inclusive" : true, "end value" : "sw2bszgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bszu", "start inclusive" : true, "end value" : "sw2bszvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bszy", "start inclusive" : true, "end value" : "sw2bszzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btpb", "start inclusive" : true, "end value" : "sw2btpczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btpf", "start inclusive" : true, "end value" : "sw2btpgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btpu", "start inclusive" : true, "end value" : "sw2btpvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btpy", "start inclusive" : true, "end value" : "sw2btpzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btrb", "start inclusive" : true, "end value" : "sw2btrczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btrf", "start inclusive" : true, "end value" : "sw2btrgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btru", "start inclusive" : true, "end value" : "sw2btrvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btry", "start inclusive" : true, "end value" : "sw2btrzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btxb", "start inclusive" : true, "end value" : "sw2btxczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btxf", "start inclusive" : true, "end value" : "sw2btxgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btxu", "start inclusive" : true, "end value" : "sw2btxvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btxy", "start inclusive" : true, "end value" : "sw2btxzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2btzb", "start inclusive" : true, "end value" : "sw2btzczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu00", "start inclusive" : true, "end value" : "sw2bu41zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu44", "start inclusive" : true, "end value" : "sw2bu45zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu4h", "start inclusive" : true, "end value" : "sw2bu4jzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu4n", "start inclusive" : true, "end value" : "sw2bu4pzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu60", "start inclusive" : true, "end value" : "sw2bu61zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu64", "start inclusive" : true, "end value" : "sw2bu65zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu6h", "start inclusive" : true, "end value" : "sw2bu6jzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu6n", "start inclusive" : true, "end value" : "sw2bu6pzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bu80", "start inclusive" : true, "end value" : "sw2bud1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bud4", "start inclusive" : true, "end value" : "sw2bud7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2budd", "start inclusive" : true, "end value" : "sw2budzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bue4", "start inclusive" : true, "end value" : "sw2bue7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bued", "start inclusive" : true, "end value" : "sw2bugzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bus4", "start inclusive" : true, "end value" : "sw2bus7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2busd", "start inclusive" : true, "end value" : "sw2busezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bush", "start inclusive" : true, "end value" : "sw2bustzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2busw", "start inclusive" : true, "end value" : "sw2busxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2buu0", "start inclusive" : true, "end value" : "sw2buu9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2buud", "start inclusive" : true, "end value" : "sw2buuezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2buuh", "start inclusive" : true, "end value" : "sw2buutzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2buuw", "start inclusive" : true, "end value" : "sw2buuxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bv00", "start inclusive" : true, "end value" : "sw2bvb3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvb8", "start inclusive" : true, "end value" : "sw2bvbczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvc0", "start inclusive" : true, "end value" : "sw2bvc3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvc8", "start inclusive" : true, "end value" : "sw2bvcczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvd0", "start inclusive" : true, "end value" : "sw2bvh9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvhd", "start inclusive" : true, "end value" : "sw2bvhezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvhh", "start inclusive" : true, "end value" : "sw2bvhtzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvhw", "start inclusive" : true, "end value" : "sw2bvhxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvk0", "start inclusive" : true, "end value" : "sw2bvk9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvkd", "start inclusive" : true, "end value" : "sw2bvkezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvkh", "start inclusive" : true, "end value" : "sw2bvktzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvkw", "start inclusive" : true, "end value" : "sw2bvkxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvs0", "start inclusive" : true, "end value" : "sw2bvs9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvsd", "start inclusive" : true, "end value" : "sw2bvsezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvsh", "start inclusive" : true, "end value" : "sw2bvstzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvsw", "start inclusive" : true, "end value" : "sw2bvsxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvu0", "start inclusive" : true, "end value" : "sw2bvu9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvuc", "start inclusive" : true, "end value" : "sw2bvuzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvv1", "start inclusive" : true, "end value" : "sw2bvv1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvv3", "start inclusive" : true, "end value" : "sw2bvv7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvv9", "start inclusive" : true, "end value" : "sw2bvv9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvvc", "start inclusive" : true, "end value" : "sw2bvvzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvy1", "start inclusive" : true, "end value" : "sw2bvy1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvy3", "start inclusive" : true, "end value" : "sw2bvy7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvy9", "start inclusive" : true, "end value" : "sw2bvy9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bvyc", "start inclusive" : true, "end value" : "sw2bvyzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2by40", "start inclusive" : true, "end value" : "sw2by7zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2byd0", "start inclusive" : true, "end value" : "sw2bynzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2byq0", "start inclusive" : true, "end value" : "sw2byqzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bys0", "start inclusive" : true, "end value" : "sw2bywzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2byy0", "start inclusive" : true, "end value" : "sw2byyzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bz40", "start inclusive" : true, "end value" : "sw2bz43zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bz48", "start inclusive" : true, "end value" : "sw2bz4czzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bz50", "start inclusive" : true, "end value" : "sw2bz53zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bz58", "start inclusive" : true, "end value" : "sw2bz5czzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzh0", "start inclusive" : true, "end value" : "sw2bzh3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzh8", "start inclusive" : true, "end value" : "sw2bzhgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzhs", "start inclusive" : true, "end value" : "sw2bzjzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzk8", "start inclusive" : true, "end value" : "sw2bzkgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzks", "start inclusive" : true, "end value" : "sw2bznzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzp1", "start inclusive" : true, "end value" : "sw2bzp1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzp3", "start inclusive" : true, "end value" : "sw2bzp7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzp9", "start inclusive" : true, "end value" : "sw2bzp9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzpc", "start inclusive" : true, "end value" : "sw2bzrzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzs8", "start inclusive" : true, "end value" : "sw2bzsgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzss", "start inclusive" : true, "end value" : "sw2bztzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzu8", "start inclusive" : true, "end value" : "sw2bzugzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2bzus", "start inclusive" : true, "end value" : "sw2bzzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp01", "start inclusive" : true, "end value" : "sw2cp01", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp03", "start inclusive" : true, "end value" : "sw2cp07zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp09", "start inclusive" : true, "end value" : "sw2cp09", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp0c", "start inclusive" : true, "end value" : "sw2cp0zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp11", "start inclusive" : true, "end value" : "sw2cp11", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp13", "start inclusive" : true, "end value" : "sw2cp17zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp1h", "start inclusive" : true, "end value" : "sw2cp1rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp20", "start inclusive" : true, "end value" : "sw2cp37zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp3h", "start inclusive" : true, "end value" : "sw2cp3rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp80", "start inclusive" : true, "end value" : "sw2cp97zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cp9h", "start inclusive" : true, "end value" : "sw2cp9rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cpb0", "start inclusive" : true, "end value" : "sw2cpc7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw2cpch", "start inclusive" : true, "end value" : "sw2cpcrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bh8", "start inclusive" : true, "end value" : "sw30bhgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bhs", "start inclusive" : true, "end value" : "sw30bjzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bk8", "start inclusive" : true, "end value" : "sw30bk8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bkb", "start inclusive" : true, "end value" : "sw30bkb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bm0", "start inclusive" : true, "end value" : "sw30bm0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bm2", "start inclusive" : true, "end value" : "sw30bm2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bm8", "start inclusive" : true, "end value" : "sw30bm8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bmb", "start inclusive" : true, "end value" : "sw30bmb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bn0", "start inclusive" : true, "end value" : "sw30bq0zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bq2", "start inclusive" : true, "end value" : "sw30bq2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bq8", "start inclusive" : true, "end value" : "sw30bq8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bqb", "start inclusive" : true, "end value" : "sw30bqczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bqf", "start inclusive" : true, "end value" : "sw30bqgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bqu", "start inclusive" : true, "end value" : "sw30bqvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bqy", "start inclusive" : true, "end value" : "sw30brzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bwb", "start inclusive" : true, "end value" : "sw30bwczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bwf", "start inclusive" : true, "end value" : "sw30bwgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bwu", "start inclusive" : true, "end value" : "sw30bwvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30bwy", "start inclusive" : true, "end value" : "sw30bxzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30byb", "start inclusive" : true, "end value" : "sw30byczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30byf", "start inclusive" : true, "end value" : "sw30bygzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30byu", "start inclusive" : true, "end value" : "sw30byvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30byy", "start inclusive" : true, "end value" : "sw30bzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30cnb", "start inclusive" : true, "end value" : "sw30cnczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30cnf", "start inclusive" : true, "end value" : "sw30cngzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30cnu", "start inclusive" : true, "end value" : "sw30cnvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30cny", "start inclusive" : true, "end value" : "sw30cpzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30cqb", "start inclusive" : true, "end value" : "sw30cqczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30cqf", "start inclusive" : true, "end value" : "sw30cqgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30cqu", "start inclusive" : true, "end value" : "sw30cqvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw30cqy", "start inclusive" : true, "end value" : "sw30crzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31000", "start inclusive" : true, "end value" : "sw31017zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3101h", "start inclusive" : true, "end value" : "sw3101rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31020", "start inclusive" : true, "end value" : "sw3103zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31060", "start inclusive" : true, "end value" : "sw31079zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3107d", "start inclusive" : true, "end value" : "sw3107ezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3107h", "start inclusive" : true, "end value" : "sw3107tzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3107w", "start inclusive" : true, "end value" : "sw3107xzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31080", "start inclusive" : true, "end value" : "sw310e9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw310ed", "start inclusive" : true, "end value" : "sw310eezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw310eh", "start inclusive" : true, "end value" : "sw310etzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw310ew", "start inclusive" : true, "end value" : "sw310exzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw310f0", "start inclusive" : true, "end value" : "sw310g9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw310gd", "start inclusive" : true, "end value" : "sw310gezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw310gh", "start inclusive" : true, "end value" : "sw310gtzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw310gw", "start inclusive" : true, "end value" : "sw310gxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31100", "start inclusive" : true, "end value" : "sw31159zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3115d", "start inclusive" : true, "end value" : "sw3115ezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3115h", "start inclusive" : true, "end value" : "sw3115tzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3115w", "start inclusive" : true, "end value" : "sw3115xzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31160", "start inclusive" : true, "end value" : "sw31179zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3117d", "start inclusive" : true, "end value" : "sw3117ezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3117h", "start inclusive" : true, "end value" : "sw3117tzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3117w", "start inclusive" : true, "end value" : "sw3117xzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3117z", "start inclusive" : true, "end value" : "sw3117z", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31192", "start inclusive" : true, "end value" : "sw31193zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31196", "start inclusive" : true, "end value" : "sw3119gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3119k", "start inclusive" : true, "end value" : "sw3119mzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3119q", "start inclusive" : true, "end value" : "sw3119zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311c2", "start inclusive" : true, "end value" : "sw311c3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311c6", "start inclusive" : true, "end value" : "sw311cgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311ck", "start inclusive" : true, "end value" : "sw311cmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311cq", "start inclusive" : true, "end value" : "sw311gzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311kp", "start inclusive" : true, "end value" : "sw311kp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311kr", "start inclusive" : true, "end value" : "sw311kr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311kx", "start inclusive" : true, "end value" : "sw311kx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311kz", "start inclusive" : true, "end value" : "sw311kz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311mp", "start inclusive" : true, "end value" : "sw311mp", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311mr", "start inclusive" : true, "end value" : "sw311mr", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311mx", "start inclusive" : true, "end value" : "sw311mx", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311mz", "start inclusive" : true, "end value" : "sw311mz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw311s0", "start inclusive" : true, "end value" : "sw311vzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31412", "start inclusive" : true, "end value" : "sw31413zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31416", "start inclusive" : true, "end value" : "sw3141gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3141k", "start inclusive" : true, "end value" : "sw3141mzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3141q", "start inclusive" : true, "end value" : "sw3141zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31432", "start inclusive" : true, "end value" : "sw31433zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31436", "start inclusive" : true, "end value" : "sw3143gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3143k", "start inclusive" : true, "end value" : "sw3143mzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3143q", "start inclusive" : true, "end value" : "sw3147zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31492", "start inclusive" : true, "end value" : "sw31493zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31496", "start inclusive" : true, "end value" : "sw3149gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3149k", "start inclusive" : true, "end value" : "sw3149mzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3149q", "start inclusive" : true, "end value" : "sw3149q", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3149s", "start inclusive" : true, "end value" : "sw3149wzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3149y", "start inclusive" : true, "end value" : "sw3149y", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314d0", "start inclusive" : true, "end value" : "sw314dnzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314dq", "start inclusive" : true, "end value" : "sw314dq", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314ds", "start inclusive" : true, "end value" : "sw314dwzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314dy", "start inclusive" : true, "end value" : "sw314dy", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314e0", "start inclusive" : true, "end value" : "sw314enzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314eq", "start inclusive" : true, "end value" : "sw314eq", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314es", "start inclusive" : true, "end value" : "sw314ezzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314g8", "start inclusive" : true, "end value" : "sw314ggzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314gs", "start inclusive" : true, "end value" : "sw314mzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314s0", "start inclusive" : true, "end value" : "sw314vzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314wn", "start inclusive" : true, "end value" : "sw314wrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314ww", "start inclusive" : true, "end value" : "sw314wzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314xn", "start inclusive" : true, "end value" : "sw314xrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw314xw", "start inclusive" : true, "end value" : "sw314zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31558", "start inclusive" : true, "end value" : "sw3155gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3155s", "start inclusive" : true, "end value" : "sw3155zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31578", "start inclusive" : true, "end value" : "sw3157gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3157s", "start inclusive" : true, "end value" : "sw3157zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315e8", "start inclusive" : true, "end value" : "sw315egzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315es", "start inclusive" : true, "end value" : "sw315ezzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315g8", "start inclusive" : true, "end value" : "sw315ggzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315gs", "start inclusive" : true, "end value" : "sw315gvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315h0", "start inclusive" : true, "end value" : "sw315umzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315us", "start inclusive" : true, "end value" : "sw315uvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315v0", "start inclusive" : true, "end value" : "sw315vmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315vs", "start inclusive" : true, "end value" : "sw315vvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw315vy", "start inclusive" : true, "end value" : "sw315zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3168n", "start inclusive" : true, "end value" : "sw3168rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw316b0", "start inclusive" : true, "end value" : "sw316b7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw316bh", "start inclusive" : true, "end value" : "sw316brzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31700", "start inclusive" : true, "end value" : "sw31707zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3170h", "start inclusive" : true, "end value" : "sw3170rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31720", "start inclusive" : true, "end value" : "sw31727zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3172h", "start inclusive" : true, "end value" : "sw3172rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31780", "start inclusive" : true, "end value" : "sw31787zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw3178h", "start inclusive" : true, "end value" : "sw3178rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317b0", "start inclusive" : true, "end value" : "sw317b7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317bh", "start inclusive" : true, "end value" : "sw317brzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317bt", "start inclusive" : true, "end value" : "sw317bt", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317bv", "start inclusive" : true, "end value" : "sw317bzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317cj", "start inclusive" : true, "end value" : "sw317cj", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317cm", "start inclusive" : true, "end value" : "sw317crzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317ct", "start inclusive" : true, "end value" : "sw317ct", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317cv", "start inclusive" : true, "end value" : "sw317czzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317fj", "start inclusive" : true, "end value" : "sw317fj", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317fm", "start inclusive" : true, "end value" : "sw317frzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317ft", "start inclusive" : true, "end value" : "sw317ft", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw317fw", "start inclusive" : true, "end value" : "sw317fxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hjb", "start inclusive" : true, "end value" : "sw31hjczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hjf", "start inclusive" : true, "end value" : "sw31hjgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hju", "start inclusive" : true, "end value" : "sw31hjvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hjy", "start inclusive" : true, "end value" : "sw31hjzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hmb", "start inclusive" : true, "end value" : "sw31hmczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hmf", "start inclusive" : true, "end value" : "sw31hmgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hmu", "start inclusive" : true, "end value" : "sw31hmvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hmy", "start inclusive" : true, "end value" : "sw31hrzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31htb", "start inclusive" : true, "end value" : "sw31htczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31htf", "start inclusive" : true, "end value" : "sw31htgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31htu", "start inclusive" : true, "end value" : "sw31htvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hty", "start inclusive" : true, "end value" : "sw31htzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hvb", "start inclusive" : true, "end value" : "sw31hvczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hvf", "start inclusive" : true, "end value" : "sw31hvgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hvu", "start inclusive" : true, "end value" : "sw31hvvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31hvy", "start inclusive" : true, "end value" : "sw31hzzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31jjb", "start inclusive" : true, "end value" : "sw31jjczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31jjf", "start inclusive" : true, "end value" : "sw31jjgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31jju", "start inclusive" : true, "end value" : "sw31jjvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31jn0", "start inclusive" : true, "end value" : "sw31jnmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31jns", "start inclusive" : true, "end value" : "sw31jnvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31jp0", "start inclusive" : true, "end value" : "sw31jpmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31jps", "start inclusive" : true, "end value" : "sw31jpvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k00", "start inclusive" : true, "end value" : "sw31k49zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k4d", "start inclusive" : true, "end value" : "sw31k4ezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k4h", "start inclusive" : true, "end value" : "sw31k4tzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k4w", "start inclusive" : true, "end value" : "sw31k4xzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k60", "start inclusive" : true, "end value" : "sw31k69zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k6d", "start inclusive" : true, "end value" : "sw31k6ezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k6h", "start inclusive" : true, "end value" : "sw31k6tzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k6w", "start inclusive" : true, "end value" : "sw31k6xzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31k80", "start inclusive" : true, "end value" : "sw31kd9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31kdd", "start inclusive" : true, "end value" : "sw31kdezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31kdh", "start inclusive" : true, "end value" : "sw31kdtzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31kdw", "start inclusive" : true, "end value" : "sw31kdxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31kf0", "start inclusive" : true, "end value" : "sw31kf9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31kfd", "start inclusive" : true, "end value" : "sw31kfezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31kfh", "start inclusive" : true, "end value" : "sw31kftzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31kfw", "start inclusive" : true, "end value" : "sw31kfxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m00", "start inclusive" : true, "end value" : "sw31m0mzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m0q", "start inclusive" : true, "end value" : "sw31m1zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m22", "start inclusive" : true, "end value" : "sw31m23zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m26", "start inclusive" : true, "end value" : "sw31m2gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m2k", "start inclusive" : true, "end value" : "sw31m2mzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m2q", "start inclusive" : true, "end value" : "sw31m49zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m4d", "start inclusive" : true, "end value" : "sw31m4ezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m4h", "start inclusive" : true, "end value" : "sw31m4tzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m4v", "start inclusive" : true, "end value" : "sw31m4zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m5j", "start inclusive" : true, "end value" : "sw31m5j", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m5m", "start inclusive" : true, "end value" : "sw31m5rzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m5t", "start inclusive" : true, "end value" : "sw31m5t", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m5v", "start inclusive" : true, "end value" : "sw31m7zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m82", "start inclusive" : true, "end value" : "sw31m83zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m86", "start inclusive" : true, "end value" : "sw31m8gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m8k", "start inclusive" : true, "end value" : "sw31m8mzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31m8q", "start inclusive" : true, "end value" : "sw31m9zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mb2", "start inclusive" : true, "end value" : "sw31mb3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mb6", "start inclusive" : true, "end value" : "sw31mbgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mbk", "start inclusive" : true, "end value" : "sw31mbmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mbq", "start inclusive" : true, "end value" : "sw31mgzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mhj", "start inclusive" : true, "end value" : "sw31mhj", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mhm", "start inclusive" : true, "end value" : "sw31mhrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mht", "start inclusive" : true, "end value" : "sw31mht", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mhv", "start inclusive" : true, "end value" : "sw31mhzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mjj", "start inclusive" : true, "end value" : "sw31mjj", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mjn", "start inclusive" : true, "end value" : "sw31mjpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mk0", "start inclusive" : true, "end value" : "sw31mm1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mm4", "start inclusive" : true, "end value" : "sw31mm5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mmh", "start inclusive" : true, "end value" : "sw31mmjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mmn", "start inclusive" : true, "end value" : "sw31mmpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31ms0", "start inclusive" : true, "end value" : "sw31mt1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mt4", "start inclusive" : true, "end value" : "sw31mt5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mth", "start inclusive" : true, "end value" : "sw31mtjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mtn", "start inclusive" : true, "end value" : "sw31mtpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mu0", "start inclusive" : true, "end value" : "sw31mv1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mv4", "start inclusive" : true, "end value" : "sw31mv5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mvh", "start inclusive" : true, "end value" : "sw31mvjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31mvn", "start inclusive" : true, "end value" : "sw31mvpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q02", "start inclusive" : true, "end value" : "sw31q03zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q06", "start inclusive" : true, "end value" : "sw31q0gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q0k", "start inclusive" : true, "end value" : "sw31q0mzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q0q", "start inclusive" : true, "end value" : "sw31q1zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q22", "start inclusive" : true, "end value" : "sw31q23zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q26", "start inclusive" : true, "end value" : "sw31q2gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q2k", "start inclusive" : true, "end value" : "sw31q2k", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q2s", "start inclusive" : true, "end value" : "sw31q2s", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q2u", "start inclusive" : true, "end value" : "sw31q2u", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q30", "start inclusive" : true, "end value" : "sw31q3hzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q3k", "start inclusive" : true, "end value" : "sw31q3k", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q3s", "start inclusive" : true, "end value" : "sw31q3s", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q3u", "start inclusive" : true, "end value" : "sw31q3u", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q40", "start inclusive" : true, "end value" : "sw31q6hzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q6k", "start inclusive" : true, "end value" : "sw31q6k", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31q6s", "start inclusive" : true, "end value" : "sw31q7zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qd8", "start inclusive" : true, "end value" : "sw31qdgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qds", "start inclusive" : true, "end value" : "sw31qezzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qf8", "start inclusive" : true, "end value" : "sw31qfgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qfs", "start inclusive" : true, "end value" : "sw31qj1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qj4", "start inclusive" : true, "end value" : "sw31qj5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qjh", "start inclusive" : true, "end value" : "sw31qjjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qjn", "start inclusive" : true, "end value" : "sw31qjpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qk0", "start inclusive" : true, "end value" : "sw31qm1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qm4", "start inclusive" : true, "end value" : "sw31qm5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qmh", "start inclusive" : true, "end value" : "sw31qmzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qqh", "start inclusive" : true, "end value" : "sw31qqzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qrh", "start inclusive" : true, "end value" : "sw31qrrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qs0", "start inclusive" : true, "end value" : "sw31qx7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qxh", "start inclusive" : true, "end value" : "sw31qxrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qy0", "start inclusive" : true, "end value" : "sw31qz7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31qzh", "start inclusive" : true, "end value" : "sw31qzrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31r48", "start inclusive" : true, "end value" : "sw31r4gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31r4s", "start inclusive" : true, "end value" : "sw31r5zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31r68", "start inclusive" : true, "end value" : "sw31r6gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31r6s", "start inclusive" : true, "end value" : "sw31r7zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rd8", "start inclusive" : true, "end value" : "sw31rdgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31re0", "start inclusive" : true, "end value" : "sw31regzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rh0", "start inclusive" : true, "end value" : "sw31rp7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rph", "start inclusive" : true, "end value" : "sw31rprzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rq0", "start inclusive" : true, "end value" : "sw31rr7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rrh", "start inclusive" : true, "end value" : "sw31rrrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rs0", "start inclusive" : true, "end value" : "sw31rsgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rt0", "start inclusive" : true, "end value" : "sw31rtzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rv0", "start inclusive" : true, "end value" : "sw31rx7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rxh", "start inclusive" : true, "end value" : "sw31rxrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31ry0", "start inclusive" : true, "end value" : "sw31rz7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw31rzh", "start inclusive" : true, "end value" : "sw31rzrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332j0", "start inclusive" : true, "end value" : "sw332jzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332m0", "start inclusive" : true, "end value" : "sw332p7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332ph", "start inclusive" : true, "end value" : "sw332przzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332q0", "start inclusive" : true, "end value" : "sw332r7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332rh", "start inclusive" : true, "end value" : "sw332rrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332t0", "start inclusive" : true, "end value" : "sw332tzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332v0", "start inclusive" : true, "end value" : "sw332x7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332xh", "start inclusive" : true, "end value" : "sw332xrzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332y0", "start inclusive" : true, "end value" : "sw332z7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw332zd", "start inclusive" : true, "end value" : "sw332zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw333j0", "start inclusive" : true, "end value" : "sw333jzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw333m0", "start inclusive" : true, "end value" : "sw333rzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw333t0", "start inclusive" : true, "end value" : "sw333tzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw333v0", "start inclusive" : true, "end value" : "sw333zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336j0", "start inclusive" : true, "end value" : "sw336j4zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336j6", "start inclusive" : true, "end value" : "sw336j6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336j8", "start inclusive" : true, "end value" : "sw336jdzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336jf", "start inclusive" : true, "end value" : "sw336jf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336n0", "start inclusive" : true, "end value" : "sw336n4zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336n6", "start inclusive" : true, "end value" : "sw336n6", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336n8", "start inclusive" : true, "end value" : "sw336ndzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336nf", "start inclusive" : true, "end value" : "sw336nf", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336p0", "start inclusive" : true, "end value" : "sw336p4zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336p6", "start inclusive" : true, "end value" : "sw336pgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336pk", "start inclusive" : true, "end value" : "sw336pmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336pq", "start inclusive" : true, "end value" : "sw336pzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336r2", "start inclusive" : true, "end value" : "sw336r3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336r6", "start inclusive" : true, "end value" : "sw336rgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336rk", "start inclusive" : true, "end value" : "sw336rmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336rq", "start inclusive" : true, "end value" : "sw336rzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336x2", "start inclusive" : true, "end value" : "sw336x3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336x6", "start inclusive" : true, "end value" : "sw336xgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336xk", "start inclusive" : true, "end value" : "sw336xmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336xq", "start inclusive" : true, "end value" : "sw336xzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336z2", "start inclusive" : true, "end value" : "sw336z3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336z6", "start inclusive" : true, "end value" : "sw336zgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336zk", "start inclusive" : true, "end value" : "sw336zmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw336zq", "start inclusive" : true, "end value" : "sw336zzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw337p2", "start inclusive" : true, "end value" : "sw337p3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw337p6", "start inclusive" : true, "end value" : "sw337pgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw337pk", "start inclusive" : true, "end value" : "sw337pmzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw337pq", "start inclusive" : true, "end value" : "sw337pzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw337r2", "start inclusive" : true, "end value" : "sw337r3zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw337r8", "start inclusive" : true, "end value" : "sw337rczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw338b4", "start inclusive" : true, "end value" : "sw338b7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw338bd", "start inclusive" : true, "end value" : "sw338bzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw338c4", "start inclusive" : true, "end value" : "sw338c7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw338cd", "start inclusive" : true, "end value" : "sw338czzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33900", "start inclusive" : true, "end value" : "sw3393zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33980", "start inclusive" : true, "end value" : "sw339czzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33d00", "start inclusive" : true, "end value" : "sw33d3zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33d44", "start inclusive" : true, "end value" : "sw33d47zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33d4d", "start inclusive" : true, "end value" : "sw33d4zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33d54", "start inclusive" : true, "end value" : "sw33d57zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33d5d", "start inclusive" : true, "end value" : "sw33dgzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dh4", "start inclusive" : true, "end value" : "sw33dh5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dhh", "start inclusive" : true, "end value" : "sw33dhjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dhn", "start inclusive" : true, "end value" : "sw33dhpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dk0", "start inclusive" : true, "end value" : "sw33dk1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dk4", "start inclusive" : true, "end value" : "sw33dk5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dkh", "start inclusive" : true, "end value" : "sw33dkjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dkn", "start inclusive" : true, "end value" : "sw33dkpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ds0", "start inclusive" : true, "end value" : "sw33ds1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ds4", "start inclusive" : true, "end value" : "sw33ds5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dsh", "start inclusive" : true, "end value" : "sw33dsjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dsn", "start inclusive" : true, "end value" : "sw33dspzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33du0", "start inclusive" : true, "end value" : "sw33du1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33du4", "start inclusive" : true, "end value" : "sw33du5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33duh", "start inclusive" : true, "end value" : "sw33dujzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33dun", "start inclusive" : true, "end value" : "sw33dupzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e00", "start inclusive" : true, "end value" : "sw33e23zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e28", "start inclusive" : true, "end value" : "sw33e2czzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e30", "start inclusive" : true, "end value" : "sw33e33zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e38", "start inclusive" : true, "end value" : "sw33e3czzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e3f", "start inclusive" : true, "end value" : "sw33e3gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e3u", "start inclusive" : true, "end value" : "sw33e3vzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e3y", "start inclusive" : true, "end value" : "sw33e7zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e9b", "start inclusive" : true, "end value" : "sw33e9czzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e9f", "start inclusive" : true, "end value" : "sw33e9gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e9u", "start inclusive" : true, "end value" : "sw33e9vzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33e9y", "start inclusive" : true, "end value" : "sw33e9zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ecb", "start inclusive" : true, "end value" : "sw33ecczzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ecf", "start inclusive" : true, "end value" : "sw33ecgzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ecu", "start inclusive" : true, "end value" : "sw33ecvzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ecy", "start inclusive" : true, "end value" : "sw33eh1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eh4", "start inclusive" : true, "end value" : "sw33eh5zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ehh", "start inclusive" : true, "end value" : "sw33ehjzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ehn", "start inclusive" : true, "end value" : "sw33ehpzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ek0", "start inclusive" : true, "end value" : "sw33ek1zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ek3", "start inclusive" : true, "end value" : "sw33ek7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ek9", "start inclusive" : true, "end value" : "sw33ek9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ekc", "start inclusive" : true, "end value" : "sw33ekzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33em1", "start inclusive" : true, "end value" : "sw33em1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33em3", "start inclusive" : true, "end value" : "sw33em7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33em9", "start inclusive" : true, "end value" : "sw33em9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33emc", "start inclusive" : true, "end value" : "sw33emzzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eq1", "start inclusive" : true, "end value" : "sw33eq1", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eq3", "start inclusive" : true, "end value" : "sw33eq7zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eq9", "start inclusive" : true, "end value" : "sw33eq9", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eqd", "start inclusive" : true, "end value" : "sw33eqezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eqh", "start inclusive" : true, "end value" : "sw33eqtzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eqw", "start inclusive" : true, "end value" : "sw33eqxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33es0", "start inclusive" : true, "end value" : "sw33ew9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ewd", "start inclusive" : true, "end value" : "sw33ewezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ewh", "start inclusive" : true, "end value" : "sw33ewtzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eww", "start inclusive" : true, "end value" : "sw33ewxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ey0", "start inclusive" : true, "end value" : "sw33ey9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eyd", "start inclusive" : true, "end value" : "sw33eyezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eyh", "start inclusive" : true, "end value" : "sw33eytzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33eyw", "start inclusive" : true, "end value" : "sw33eyxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s1b", "start inclusive" : true, "end value" : "sw33s1czzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s1f", "start inclusive" : true, "end value" : "sw33s1gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s1u", "start inclusive" : true, "end value" : "sw33s1vzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s1y", "start inclusive" : true, "end value" : "sw33s1zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s3b", "start inclusive" : true, "end value" : "sw33s3czzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s3f", "start inclusive" : true, "end value" : "sw33s3gzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s3u", "start inclusive" : true, "end value" : "sw33s3vzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s3y", "start inclusive" : true, "end value" : "sw33s7zzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33s9b", "start inclusive" : true, "end value" : "sw33s9b", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sd0", "start inclusive" : true, "end value" : "sw33sd0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sd2", "start inclusive" : true, "end value" : "sw33sd2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sd8", "start inclusive" : true, "end value" : "sw33sd8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sdb", "start inclusive" : true, "end value" : "sw33sdb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33se0", "start inclusive" : true, "end value" : "sw33se0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33se2", "start inclusive" : true, "end value" : "sw33se2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33se8", "start inclusive" : true, "end value" : "sw33se8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33seb", "start inclusive" : true, "end value" : "sw33seb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sh0", "start inclusive" : true, "end value" : "sw33sn9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33snd", "start inclusive" : true, "end value" : "sw33snezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33snh", "start inclusive" : true, "end value" : "sw33sntzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33snw", "start inclusive" : true, "end value" : "sw33snxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sq0", "start inclusive" : true, "end value" : "sw33sq9zzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sqd", "start inclusive" : true, "end value" : "sw33sqezzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sqh", "start inclusive" : true, "end value" : "sw33sqtzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sqw", "start inclusive" : true, "end value" : "sw33sqxzzz", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ss0", "start inclusive" : true, "end value" : "sw33ss0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ss2", "start inclusive" : true, "end value" : "sw33ss2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ss8", "start inclusive" : true, "end value" : "sw33ss8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33ssb", "start inclusive" : true, "end value" : "sw33ssb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33st0", "start inclusive" : true, "end value" : "sw33st0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33st2", "start inclusive" : true, "end value" : "sw33st2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33st8", "start inclusive" : true, "end value" : "sw33st8", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33stb", "start inclusive" : true, "end value" : "sw33stb", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sw0", "start inclusive" : true, "end value" : "sw33sw0", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sw2", "start inclusive" : true, "end value" : "sw33sw2", "end inclusive" : true } }
        },
        {
          "equality conditions" : {},
          "range conditions" : { "info.point" : { "start value" : "sw33sw8", "start inclusive" : true, "end value" : "sw33sw8", "end inclusive" : true } }
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
        "value" : {"coordinates":[[23.5943,35.2481],[24.4564,35.455]],"type":"LineString"}
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