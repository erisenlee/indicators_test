from app.runner import Dataset
# from app.core import Indicators
# from itertools import chain
# import pandas as pd
# from math import ceil

# def get_average_time(totaltime, order_count):
#     if not totaltime:
#         return '0'
#     minu , sec = divmod(totaltime/order_count,60)
#     return f"{int(minu)}.{ceil(sec)}"





def query(query_dict):
# from . import core
    data = Dataset('dbdevelop',query_dict=query_dict)
    # indicators_dataset = data.get_dataset()
    # assert_data = data.get_assert_data()
    # try:
    #     if not isinstance(assert_data, pd.DataFrame):
    #         raise ValueError('not dataframe')
    # except Exception as e:
    #     print(f'[!] assert data is not valid dataframe! {e}')
    #     # print(dataset.get_dataset().shape)
    # i = Indicators(indicators_dataset)
    # basic = i.get_indicators_value()
    # timeout = i.get_all_timeout()
    # fraud = i.get_all_fraud()
    # duration = i.get_all_duration()
    # good_order = data.good_order_dict()
    
    # iterchain = chain(
    #     basic.items(),
    #     timeout.items(),
    #     fraud.items(),
    #     duration.items(),
    #     good_order.items()
    # )
    # print(f'################start test######################')
    service_data = data.get_service_data()
    return service_data
    # print(service_data)


    # for group_key, name_tuple in iterchain:
    #     print(f'>>>>assert start {group_key}<<<<<<<<<\n')
    #     for key ,value in name_tuple._asdict().items():
    #         assertvalue = assert_data.get(key, None)
    #         result = assertvalue.sum() if assertvalue is not None else 'not found'
    #         if value != result:
    #             print(f'{key} :\n{value} <--> {result}')
    #             print('===========================') 
    # # basic_indicators = basic['basic']
    # day_vaild_order_count = basic_indicators.day_valid_order_count
    # noon_vaild_order_count = basic_indicators.noon_valid_order_count
    # night_vaild_order_count = basic_indicators.night_valid_order_count

    # schedule_time = duration['schedule_time']

    # day_schedule_time = get_average_time(schedule_time.day_schedule_time, day_vaild_order_count)
    # noon_schedule_time = get_average_time(schedule_time.noon_schedule_time, noon_vaild_order_count)
    # night_schedule_time = get_average_time(schedule_time.night_schedule_time, night_vaild_order_count)
    
    # print(f'{day_schedule_time} {noon_schedule_time} {night_schedule_time}')
    # print(re)