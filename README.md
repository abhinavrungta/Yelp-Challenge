# Yelp-Challenge

== Feature Set ==

root                                                                            
 |-- user_id: string (nullable = true)
 |-- name: string (nullable = true)
 |-- review_count: long (nullable = true)
 |-- average_stars: double (nullable = true)
 |-- votes_cool: long (nullable = true)
 |-- votes_funny: long (nullable = true)
 |-- votes_useful: long (nullable = true)
 |-- months_yelping: double (nullable = true)
 |-- elite: array (nullable = true)
 |    |-- element: long (containsNull = true)
 |-- category: string (nullable = true)
 |-- cat_business_count: long (nullable = false)
 |-- cat_review_count: long (nullable = false)
 |-- cat_avg_review_len: double (nullable = true)
 |-- cat_avg_stars: double (nullable = true)
