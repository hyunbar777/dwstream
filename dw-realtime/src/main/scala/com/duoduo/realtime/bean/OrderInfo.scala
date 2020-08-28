package com.duoduo.realtime.bean

/**
 * Author z
 * Date 2020-08-28 21:35:17
 */
case class OrderInfo(
                      id: Long,
                      province_id: Long,
                      order_status: String,
                      user_id: Long,
                      final_total_amount: Double,
                      benefit_reduce_amount: Double,
                      original_total_amount: Double,
                      feight_fee: Double,
                      expire_time: String,
                      create_time: String,
                      operate_time: String,
                      var create_date: String,
                      var create_hour: String,
                      var if_first_order:String,
                      var province_name:String,
                      var province_area_code:String,
                      var user_age_group:String,
                      var user_gender:String
                    )