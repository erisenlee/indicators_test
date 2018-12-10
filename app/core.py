import pandas as pd
from collections import namedtuple
from math import ceil


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
        # cancel = df['delivery_status'] == '配送失败'
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
            # 'cancel',
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
            # cancel=cancel,
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

        complete_df = self.df[basic_indicators.complete]
        valid_df = self.df[basic_indicators.valid]
        exception_cancel_df = self.df[basic_indicators.exception_cancel]
        fraud_df = self.df[basic_indicators.fraud]
        time_out_df = self.df[basic_indicators.time_out]
        serious_overtime_df = self.df[basic_indicators.serious_overtime]

        total = self.df.shape[0]
        complete = complete_df.shape[0]
        valid = valid_df.shape[0]
        # cancel = self.df[basic_indicators.cancel].shape[0]
        exception_cancel = self.df[basic_indicators.exception_cancel].shape[0]
        abnormal_cancel_agent_order_count = exception_cancel_df[basic_indicators.abnormal_cancel_agent_order_count].shape[0]
        abnormal_cancel_customer_order_count = exception_cancel_df[basic_indicators.abnormal_cancel_customer_order_count].shape[0]
        abnormal_cancel_merchants_order_count = exception_cancel_df[basic_indicators.abnormal_cancel_merchants_order_count].shape[0]
        total_guest_amount = valid_df['order_amount'].sum()
        avg_order_price = round(total_guest_amount / valid, 2)
        valid_rider = self.valid_rider(valid_df)
        fraud = fraud_df.shape[0]
        time_out = time_out_df.shape[0]
        serious_overtime = serious_overtime_df.shape[0]

        #率值指标
        time_out_rate = round(time_out / total, 4)
        serious_overtime_rate = round(serious_overtime / total, 4)
        fraud_rate = round(fraud / total, 4)
        exception_cancel_rate = round(exception_cancel/total, 4)
        

        IndicatorsValue = namedtuple('IndicatorsValue', [
            'accept_order_count',
            'complete_order_count',
            'valid_order_count',
            'abnormal_cancel_order_count',
            'abnormal_cancel_order_rate',
            'abnormal_cancel_merchants_order_count',
            'abnormal_cancel_customer_order_count',
            'abnormal_cancel_agent_order_count',
            'valid_staff_count',
            'total_guest_amount',
            'avg_order_price',
            'fraud_order_count',
            'fraud_rate',
            'timeout_order_count',
            'timeout_rate',
            'serious_timeout_order_count',
            'serious_overtime_rate',

            
        ])

        value = IndicatorsValue(
            accept_order_count=total,
            complete_order_count=complete,
            valid_order_count=valid,
            abnormal_cancel_order_count=exception_cancel,
            abnormal_cancel_order_rate=exception_cancel_rate,
            abnormal_cancel_agent_order_count=abnormal_cancel_agent_order_count,
            abnormal_cancel_customer_order_count=abnormal_cancel_customer_order_count,
            abnormal_cancel_merchants_order_count=abnormal_cancel_merchants_order_count,
            valid_staff_count=valid_rider,
            total_guest_amount=total_guest_amount,
            avg_order_price=avg_order_price,
            fraud_order_count=fraud,
            fraud_rate=fraud_rate,
            timeout_order_count=time_out,
            timeout_rate=time_out_rate,
            serious_timeout_order_count=serious_overtime,
            serious_overtime_rate=serious_overtime_rate


            
        )

        return {'basic':value}

    def _get_average_time(self,totaltime, order_count):
        if not totaltime:
            return '0'
        minu , sec = divmod(totaltime/order_count,60)
        return f"{int(minu)}.{ceil(sec)}"
        
    
    def get_all_duration(self):
        basic_indicators = self.indicators_basic(self.df)
        
        day_valid_df = self.df[basic_indicators.valid]
        valid_count = day_valid_df[day_valid_df['waybill_type'] == '普通单'].shape[0]


        #调度时长
        schedule_time = self._get_druation(day_valid_df, 'assign_time', 'accept_time')
        average_schedule_time = self._get_average_time(schedule_time,valid_count)
        #到店时长
        toshop_time = self._get_druation(day_valid_df,'toshop_time','assign_time')
        average_toshop_time = self._get_average_time(toshop_time,valid_count)
        #出餐时长
        meal_time = self._get_druation(day_valid_df,'take_time','order_time')
        average_meal_time = self._get_average_time(meal_time,valid_count)
        #取餐时长
        take_time = self._get_druation(day_valid_df, 'take_time', 'toshop_time')
        average_take_time = self._get_average_time(take_time,valid_count)
        #送餐时长
        send_time = self._get_druation(day_valid_df, 'arrive_time', 'take_time')
        average_send_time = self._get_average_time(send_time,valid_count)
        #配送时长
        distribution_time = self._get_druation(day_valid_df,'arrive_time','assign_time')
        average_distribution_time = self._get_average_time(distribution_time,valid_count)
        #考核时长
        assessment_time = self._get_druation(day_valid_df,'assess_time','order_time')
        average_assessment_time = self._get_average_time(assessment_time, valid_count)
        Duration = namedtuple('Duration', [
            'schedule_time',
            'average_schedule_time',
            'to_shop_time',
            'average_toshop_time',
            'meal_time',
            'average_meal_time',
            'take_time',
            'average_take_time',
            'send_time',
            'average_send_time',
            'distribution_time',
            'average_distribution_time',
            'assessment_time',
            'average_assessment_time',
        ])

        result = Duration(
            schedule_time=schedule_time,
            to_shop_time=toshop_time,
            meal_time=meal_time,
            take_time=take_time,
            send_time=send_time,
            distribution_time=distribution_time,
            assessment_time=assessment_time,
            average_schedule_time=average_schedule_time,
            average_toshop_time=average_toshop_time,
            average_meal_time=average_meal_time,
            average_take_time=average_take_time,
            average_send_time=average_send_time,
            average_distribution_time=average_distribution_time,
            average_assessment_time=average_assessment_time
        )

        return {'duration':result}

    # def indicators_time(self, df):
        
    #     early1 = (df['waybill_type'] =='普通单') & (df['order_time'].dt.time.between(self._get_time(7),self._get_time(9)))
    #     early2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(self._get_time(7),self._get_time(9)))
        
    #     noon1 =  (df['waybill_type'] =='普通单') & (df['order_time'].dt.time.between(pd.to_datetime('10:30:00').time(),pd.to_datetime('12:30:00').time() ))
    #     noon2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(pd.to_datetime('10:30:00').time(),pd.to_datetime('12:30:00').time() ))
        
    #     moon1 =  (df['waybill_type'] =='普通单') & (df['order_time'].dt.time.between(pd.to_datetime('17:00:00').time(),pd.to_datetime('20:00:00').time()))
    #     moon2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(self._get_time(17), self._get_time(20)))
        
    #     night_snack_one1 = (df['waybill_type'] == '普通单') & (df['order_time'].dt.time.between(self._get_time(22),pd.to_datetime('23:59:59').time()))
    #     night_snack_one2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(self._get_time(22),pd.to_datetime('23:59:59').time()))

    #     night_snack_two1 = (df['waybill_type'] == '普通单') & (df['order_time'].dt.time.between(self._get_time(0),self._get_time(6)))
    #     night_snack_two2 = (df['waybill_type'] == '预订单') & (df['expect_arrive_time'].dt.time.between(self._get_time(0),self._get_time(6)))
        
    #     early = (early1 | early2)
    #     noon = (noon1 | noon2)
    #     moon = (moon1 | moon2)
    #     night_snack_one = (night_snack_one1 | night_snack_one2)
    #     night_snack_two = (night_snack_two1 | night_snack_two2)
    #     other_time = ~(early & noon & moon & night_snack_one & night_snack_two)

    #     IndicatorsTime = namedtuple('IndicatorsTime', [
    #         'early',
    #         'noon',
    #         'moon',
    #         'night_snack_one',
    #         'night_snack_two',
    #         'other_time'
    #     ])

    #     return IndicatorsTime(
    #         early=early,
    #         noon=noon,
    #         moon=moon,
    #         night_snack_one=night_snack_one,
    #         night_snack_two=night_snack_two,
    #         other_time=other_time
    #     )

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