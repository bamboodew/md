数据倾斜：检查是否有数据倾斜的问题，特别是在 GROUP BY 和 JOIN 操作中。

广播变量：如果 create_time_list 和 signed_list 的数据量较小，可以考虑使用 Spark 的广播变量来减少 Shuffle 操作。???
