import pandas as pd
from collections import namedtuple


class Indicators:
    def __init__(self, dataset):
        if not isinstance(dataset, pd.DataFrame):
            raise TypeError(f'{dataset} is not DataFrame.')
        self.df = dataset

    def group(self, *key):
        gb = pd.groupby(list(key))
        return gb

    def valid_rider(self, df):
        number = df.groupby(['ele_courier_id']).groups
        return len(number)

    def indicators_basic(self,df):
        complete = df['delivery_status'] == '配送成功'
        valid = (df['delivery_status'] == '配送成功') & (df['fraud_flag'] != '欺诈单')
        cancel = df['delivery_status'] == '配送失败'
        exception_cancel = (df['excption_cancel_flag'] != '正常') & (df['excption_cancel_flag'] != '异常取消')
        # exception_cancel = df['excption_cancel_flag'].str.startswith('超')
        abnormal_cancel_customer_order_count = df['excption_cancel_reason'] == '用户'
        abnormal_cancel_merchants_order_count = df['excption_cancel_reason'] == '商户'
        abnormal_cancel_agent_order_count = df['excption_cancel_reason'] == '配送商'
        fraud = df['fraud_flag'] == '欺诈单'
        time_out = df['timeout_flag'] == '超时'
        serious_overtime = df['serious_overtime'] != 0 

        BasicIndicators = namedtuple('BasicIndicators', [
            'complete',
            'valid',
            'cancel',
            'exception_cancel',
            'fraud',
            'time_out',
            'serious_overtime',
            'abnormal_cancel_customer_order_count',
            'abnormal_cancel_merchants_order_count',
            'abnormal_cancel_agent_order_count'
        ])
        basic_indicatos = BasicIndicators(
            complete=complete,
            valid=valid,
            cancel=cancel,
            exception_cancel=exception_cancel,
            fraud=fraud,
            time_out=time_out,
            serious_overtime=serious_overtime,
            abnormal_cancel_agent_order_count=abnormal_cancel_agent_order_count,
            abnormal_cancel_customer_order_count=abnormal_cancel_customer_order_count,
            abnormal_cancel_merchants_order_count=abnormal_cancel_merchants_order_count
        )
        return basic_indicatos

    def get_indicators_value(self):
        basic_indicators = self.indicators_basic(self.df)
        time_indicators = self.indicators_time(self.df)
        # duration_indicators = self.indicators_duration(self.df)


        complete_df = self.df[basic_indicators.complete]
        valid_df = self.df[basic_indicators.valid]

        # normal_valid = valid_df[valid_df['waybill_type'] == '普通单'].shape[0]
        # print(valid_df)
        exception_cancel_df = self.df[basic_indicators.exception_cancel]
        #全天
        total = self.df.shape[0]
        complete = complete_df.shape[0]
        valid = valid_df.shape[0]
        # cancel = self.df[basic_indicators.cancel].shape[0]
        exception_cancel = self.df[basic_indicators.exception_cancel].shape[0]
        abnormal_cancel_agent_order_count = exception_cancel_df[basic_indicators.abnormal_cancel_agent_order_count].shape[0]
        abnormal_cancel_customer_order_count = exception_cancel_df[basic_indicators.abnormal_cancel_customer_order_count].shape[0]
        abnormal_cancel_merchants_order_count = exception_cancel_df[basic_indicators.abnormal_cancel_merchants_order_count].shape[0]
        # serious_overtime = serious_overtime_df[basic_indicators.serious_overtime].shape[0]
        total_guest_amount = valid_df['order_amount'].sum()
        # avg_order_price = valid_df['order_amount'].sum() / valid
        valid_rider = self.valid_rider(valid_df)

        
       
        #早餐单量统计
        early_total = self.df[time_indicators.early].shape[0]
        early_complete = complete_df[time_indicators.early].shape[0]
        early_valid = valid_df[time_indicators.early].shape[0]
        #早餐有效骑手数
        early_valid_rider = self.valid_rider(valid_df[time_indicators.early])
        
        #午高峰统计
        noon_total = self.df[time_indicators.noon].shape[0]
        noon_complete = complete_df[time_indicators.noon].shape[0]
        noon_valid = valid_df[time_indicators.noon].shape[0]
        #午高峰有效骑手数
        noon_valid_rider = self.valid_rider(valid_df[time_indicators.noon])

        #晚高峰统计
        moon_total = self.df[time_indicators.moon].shape[0]
        moon_complete = complete_df[time_indicators.moon].shape[0]
        moon_valid = valid_df[time_indicators.moon].shape[0]
        #晚高峰有效骑手数
        moon_valid_rider = self.valid_rider(valid_df[time_indicators.moon])

        #夜宵单1统计
        night_snack_one_total = self.df[time_indicators.night_snack_one].shape[0]
        night_snack_one_complete = complete_df[time_indicators.night_snack_one].shape[0]
        night_snack_one_valid = valid_df[time_indicators.night_snack_one].shape[0]
        
        #夜宵单1有效骑手数
        night_snack_one_valid_rider = self.valid_rider(valid_df[time_indicators.night_snack_one])

        #夜宵单2统计
        night_snack_two_total = self.df[time_indicators.night_snack_two].shape[0]
        night_snack_two_complete = complete_df[time_indicators.night_snack_two].shape[0]
        night_snack_two_valid = valid_df[time_indicators.night_snack_two].shape[0]
        
        #夜宵单2有效骑手数
        night_snack_two_valid_rider = self.valid_rider(valid_df[time_indicators.night_snack_two])

        
        

        IndicatorsValue = namedtuple('IndicatorsValue', [
            'day_accept_order_count',
            'day_complete_order_count',
            'day_valid_order_count',
            'abnormal_cancel_order_count',
            'abnormal_cancel_merchants_order_count',
            'abnormal_cancel_customer_order_count',
            'abnormal_cancel_agent_order_count',
            'total_guest_amount',
            'day_valid_staff_count',

            'early_accept_order_count',
            'early_complete_order_count',
            'early_valid_order_count',
            'early_valid_staff_count',

            'noon_accept_order_count',
            'noon_complete_order_count',
            'noon_valid_order_count',
            'noon_valid_staff_count',

            'night_accept_order_count',
            'night_complete_order_count',
            'night_valid_order_count',
            'night_valid_staff_count',

            'midnight_one_accept_order_count',
            'midnight_one_complete_order_count',
            'midnight_one_valid_order_count',
            'midnight_one_valid_staff_count',

            'midnight_two_accept_order_count',
            'midnight_two_complete_order_count',
            'midnight_two_valid_order_count',
            'midnight_two_valid_staff_count'
        ])

        value = IndicatorsValue(
            day_accept_order_count=total,
            day_complete_order_count=complete,
            day_valid_order_count=valid,
            abnormal_cancel_order_count=exception_cancel,
            abnormal_cancel_agent_order_count=abnormal_cancel_agent_order_count,
            abnormal_cancel_customer_order_count=abnormal_cancel_customer_order_count,
            abnormal_cancel_merchants_order_count=abnormal_cancel_merchants_order_count,
            day_valid_staff_count=valid_rider,
            total_guest_amount=total_guest_amount,

            early_accept_order_count=early_total,
            early_complete_order_count=early_complete,
            early_valid_order_count=early_valid,
            early_valid_staff_count=early_valid_rider,

            noon_accept_order_count=noon_total,
            noon_complete_order_count=noon_complete,
            noon_valid_order_count=noon_valid,
            noon_valid_staff_count=noon_valid_rider,

            night_accept_order_count=moon_total,
            night_complete_order_count=moon_complete,
            night_valid_order_count=moon_valid,
            night_valid_staff_count=moon_valid_rider,

            midnight_one_accept_order_count= night_snack_one_total,
            midnight_one_complete_order_count=night_snack_one_complete,
            midnight_one_valid_order_count=night_snack_one_valid,
            midnight_one_valid_staff_count=night_snack_one_valid_rider,

            midnight_two_accept_order_count=night_snack_two_total,
            midnight_two_complete_order_count=night_snack_two_complete,
            midnight_two_valid_order_count=night_snack_two_valid,
            midnight_two_valid_staff_count=night_snack_two_valid_rider
        )

        return {'basic':value}

    def get_all_fraud(self):
        basic_indicators = self.indicators_basic(self.df)
        time_indicators = self.indicators_time(self.df)
        fraud_df = self.df[basic_indicators.fraud]

         #欺诈单量
        day_fraud = fraud_df[basic_indicators.fraud].shape[0]
        early_fraud = fraud_df[time_indicators.early].shape[0]
        noon_fraud = fraud_df[time_indicators.noon].shape[0]
        moon_fraud = fraud_df[time_indicators.moon].shape[0]
        night_snack_one_fraud = fraud_df[time_indicators.night_snack_one].shape[0]
        night_snack_two_fraud = fraud_df[time_indicators.night_snack_two].shape[0]

        Fraud = get_nametuple('fraud',
            'day_fraud_order_count',
            'early_fraud_order_count',
            'noon_fraud_order_count',
            'night_fraud_order_count',
            'midnight_one_fraud_order_count',
            'midnight_two_fraud_order_count'
        )
        fraud = Fraud(
            day_fraud_order_count=day_fraud,
            early_fraud_order_count=early_fraud,
            noon_fraud_order_count=noon_fraud,
            night_fraud_order_count=moon_fraud,
            midnight_one_fraud_order_count=night_snack_one_fraud,
            midnight_two_fraud_order_count=night_snack_two_fraud
        )
        return {
            'fraud':fraud
        }

    def get_all_timeout(self):
        basic_indicators = self.indicators_basic(self.df)
        time_indicators = self.indicators_time(self.df)
        time_out_df = self.df[basic_indicators.time_out]
        serious_overtime_df = self.df[basic_indicators.serious_overtime]
        #超时单量
        day_timeout = time_out_df[basic_indicators.time_out].shape[0]
        early_timeout = time_out_df[time_indicators.early].shape[0]
        noon_timeout = time_out_df[time_indicators.noon].shape[0]
        moon_timeout = time_out_df[time_indicators.moon].shape[0]
        night_snack_one_timeout = time_out_df[time_indicators.night_snack_one].shape[0]
        night_snack_two_timeout = time_out_df[time_indicators.night_snack_two].shape[0]


        Time_out = get_nametuple('time_out',
            'day_timeout_order_count',
            'early_timeout_order_count',
            'noon_timeout_order_count',
            'night_timeout_order_count',
            'midnight_one_timeout_order_count',
            'midnight_two_timeout_order_count'

        )
        timeout = Time_out(
            day_timeout_order_count=day_timeout,
            early_timeout_order_count=early_timeout,
            noon_timeout_order_count=noon_timeout,
            night_timeout_order_count=moon_timeout,
            midnight_one_timeout_order_count=night_snack_one_timeout,
            midnight_two_timeout_order_count=night_snack_two_timeout
        )

        #严重超时单量
        day_serious_overtime = serious_overtime_df[basic_indicators.time_out].shape[0]
        early_serious_overtime = serious_overtime_df[time_indicators.early].shape[0]
        noon_serious_overtime = serious_overtime_df[time_indicators.noon].shape[0]
        moon_serious_overtime = serious_overtime_df[time_indicators.moon].shape[0]
        night_snack_one_serious_overtime = serious_overtime_df[time_indicators.night_snack_one].shape[0]
        night_snack_two_serious_overtime = serious_overtime_df[time_indicators.night_snack_two].shape[0]
        
        Serious_overtime = get_nametuple('serious_overtime',
            'day_serious_timeout_order_count',
            'early_serious_timeout_order_count',
            'noon_serious_timeout_order_count',
            'night_serious_timeout_order_count',
            'midnight_one_serious_timeout_order_count',
            'midnight_two_serious_timeout_order_count'
        )
        serious_overtime = Serious_overtime(
            day_serious_timeout_order_count=day_serious_overtime,
            early_serious_timeout_order_count=early_serious_overtime,
            noon_serious_timeout_order_count=noon_serious_overtime,
            night_serious_timeout_order_count=moon_serious_overtime,
            midnight_one_serious_timeout_order_count=night_snack_one_serious_overtime,
            midnight_two_serious_timeout_order_count=night_snack_two_serious_overtime
        )    

        return {
            'timeout': timeout,
            'serious_overtime':serious_overtime
        }

    def get_all_duration(self):
        basic_indicators = self.indicators_basic(self.df)
        time_indicators = self.indicators_time(self.df)
        
        day_valid_df = self.df[basic_indicators.valid]
        noon_valid_df = day_valid_df[time_indicators.noon]
        night_valid_df = day_valid_df[time_indicators.moon]

        #调度时长
        day_schedule_time = self._get_druation(day_valid_df, 'assign_time', 'accept_time')
        noon_schdule_time = self._get_druation(noon_valid_df,'assign_time','accept_time')
        night_schdule_time = self._get_druation(night_valid_df,'assign_time','accept_time')

        Schedule_time = get_nametuple('schedule_time',
            'day_schedule_time',
            'noon_schedule_time',
            'night_schedule_time',
        )

        schedule_time = Schedule_time(
            day_schedule_time=day_schedule_time,
            noon_schedule_time=noon_schdule_time,
            night_schedule_time=night_schdule_time,
        )

        #到店时长
        day_toshop_time = self._get_druation(day_valid_df,'toshop_time','assign_time')
        noon_toshop_time = self._get_druation(noon_valid_df,'toshop_time','assign_time')
        night_toshop_time = self._get_druation(night_valid_df,'toshop_time','assign_time')
        Toshop_time = namedtuple('Toshop_time', [
            'day_to_shop_time',
            'noon_to_shop_time',
            'night_to_shop_time',
        ])
        toshop_time = Toshop_time(
            day_to_shop_time=day_toshop_time,
            noon_to_shop_time=noon_toshop_time,
            night_to_shop_time=night_toshop_time
        )

        #出餐时长
        day_meal_time = self._get_druation(day_valid_df,'take_time','order_time')
        noon_meal_time = self._get_druation(noon_valid_df,'take_time','order_time')
        night_meal_time = self._get_druation(night_valid_df, 'take_time', 'order_time')
        Meal_time = namedtuple('Meal_time', [
            'day_meal_time',
            'noon_meal_time',
            'night_meal_time'
        ])
        meal_time = Meal_time(
            day_meal_time=day_meal_time,
            noon_meal_time=noon_meal_time,
            night_meal_time=night_meal_time
        )

        #取餐时长
        day_take_time = self._get_druation(day_valid_df, 'take_time', 'toshop_time')
        noon_take_time = self._get_druation(noon_valid_df, 'take_time', 'toshop_time')
        night_take_time = self._get_druation(night_valid_df, 'take_time', 'toshop_time')
        Take_time = namedtuple('Take_time', [
            'day_take_time',
            'noon_take_time',
            'night_take_time'
        ])
        take_time = Take_time(
            day_take_time=day_take_time,
            noon_take_time=noon_take_time,
            night_take_time=night_take_time
        )
        #送餐时长
        day_send_time = self._get_druation(day_valid_df, 'arrive_time', 'take_time')
        noon_send_time = self._get_druation(noon_valid_df, 'arrive_time', 'take_time')
        night_send_time = self._get_druation(night_valid_df, 'arrive_time', 'take_time')
        Send_time = namedtuple('Send_time', [
            'day_send_time',
            'noon_send_time',
            'night_send_time'
        ])
        send_time = Send_time(
            day_send_time=day_send_time,
            noon_send_time=noon_send_time,
            night_send_time=night_send_time
        )

        #配送时长
        day_distribution_time = self._get_druation(day_valid_df,'arrive_time','assign_time')
        noon_distribution_time = self._get_druation(noon_valid_df,'arrive_time','assign_time')
        night_distribution_time = self._get_druation(night_valid_df, 'arrive_time', 'assign_time')
        Distribution_time = namedtuple('Distribution_time', [
            'day_distribution_time',
            'noon_distribution_time',
            'night_distribution_time'
        ])
        distribution_time = Distribution_time(
            day_distribution_time=day_distribution_time,
            noon_distribution_time=noon_distribution_time,
            night_distribution_time=night_distribution_time
        )

        #考核时长
        day_assessment_time = self._get_druation(day_valid_df,'assess_time','order_time')
        noon_assessment_time = self._get_druation(noon_valid_df,'assess_time','order_time')
        night_assessment_time = self._get_druation(night_valid_df, 'assess_time', 'order_time')
        Assessment_time = namedtuple('Assessment_time', [
            'day_assessment_time',
            'noon_assessment_time',
            'night_assessment_time'
        ])
        assessment_time = Assessment_time(
            day_assessment_time=day_assessment_time,
            noon_assessment_time=noon_assessment_time,
            night_assessment_time=night_assessment_time
        )

        return {
            'schedule_time': schedule_time,
            'to_shop_time': toshop_time,
            'meal_time': meal_time,
            'take_time': take_time,
            'send_time': send_time,
            'distribution_time': distribution_time,
            'assessment_time':assessment_time
        }

    def indicators_time(self, df):
        
        early1 = (df['waybill_type'] =='普通单') & (df['order_time'].dt.time.between(self._get_time(7),self._get_time(9)))
        early2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(self._get_time(7),self._get_time(9)))
        
        noon1 =  (df['waybill_type'] =='普通单') & (df['order_time'].dt.time.between(pd.to_datetime('10:30:00').time(),pd.to_datetime('12:30:00').time() ))
        noon2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(pd.to_datetime('10:30:00').time(),pd.to_datetime('12:30:00').time() ))
        
        moon1 =  (df['waybill_type'] =='普通单') & (df['order_time'].dt.time.between(pd.to_datetime('17:00:00').time(),pd.to_datetime('20:00:00').time()))
        moon2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(self._get_time(17), self._get_time(20)))
        
        night_snack_one1 = (df['waybill_type'] == '普通单') & (df['order_time'].dt.time.between(self._get_time(22),pd.to_datetime('23:59:59').time()))
        night_snack_one2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(self._get_time(22),pd.to_datetime('23:59:59').time()))

        night_snack_two1 = (df['waybill_type'] == '普通单') & (df['order_time'].dt.time.between(self._get_time(0),self._get_time(6)))
        night_snack_two2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(self._get_time(0),self._get_time(6)))
        
        early = (early1 | early2)
        noon = (noon1 | noon2)
        moon = (moon1 | moon2)
        night_snack_one = (night_snack_one1 | night_snack_one2)
        night_snack_two = (night_snack_two1 | night_snack_two2)

        IndicatorsTime = namedtuple('IndicatorsTime', [
            'early',
            'noon',
            'moon',
            'night_snack_one',
            'night_snack_two'
        ])

        return IndicatorsTime(
            early=early,
            noon=noon,
            moon=moon,
            night_snack_one=night_snack_one,
            night_snack_two=night_snack_two
        )

    def _get_time(self, int_time):
        return pd.to_datetime(f'{str(int_time)}:00:00').time()


    def _get_druation(self, valid_df, k1, k2):
        df = valid_df[valid_df['waybill_type'] == '普通单']
        duration = df[k1] - df[k2]
        result = duration.dt.total_seconds().astype('int64').sum()
        return result
        

def get_nametuple(name, *args,**kwargs):
    if not isinstance(name, str):
        raise TypeError('argument {} is not str'.format(name))
    if not args and not kwargs:
        raise ValueError('no fields')
    if kwargs:
        Name = namedtuple(name.capitalize(), kwargs.keys())
        return Name._make(kwargs.values())
    
    return namedtuple(name.capitalize(),list(args))


if __name__ == "__main__":
    day = get_nametuple('day', 'one', 'two')
    a = 'sss'
    print(dir(a))        