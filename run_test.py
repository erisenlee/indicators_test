from app.runner import Dataset
from app.core import Indicators
from itertools import chain
import pandas as pd



def main():
# from . import core
    data = Dataset('dbtest')
    indicators_dataset = data.get_waybill_df()
    assert_data = data.get_statistical_data()
    try:
        if not isinstance(assert_data, pd.DataFrame):
            raise ValueError('not dataframe')
    except Exception as e:
        print(f'[!] assert data is not valid dataframe! {e}')
        # print(dataset.get_dataset().shape)
    i = Indicators(indicators_dataset)
    basic = i.get_indicators_value()
    duration = i.get_all_duration()
    good_order = data.good_order_dict()
    
    iterchain = chain(
        basic.items(),
        duration.items(),
        good_order.items()
    )
    print(f'################start test######################')
    service_data = data.get_service_data()
    print(service_data)


    for group_key, name_tuple in iterchain:
        print(f'>>>>assert start {group_key}<<<<<<<<<\n')
        for key ,value in name_tuple._asdict().items():
            assertvalue = assert_data.get(key, None)
            result = assertvalue.sum() if assertvalue is not None else 'not found'
            # if value != result:
            print(f'{key} :\n{value} <--> {result}')
            print('===========================') 
    # basic_indicators = basic['basic']
    # day_vaild_order_count = basic_indicators.day_valid_order_count
    # noon_vaild_order_count = basic_indicators.noon_valid_order_count
    # night_vaild_order_count = basic_indicators.night_valid_order_count

    # schedule_time = duration['schedule_time']

    # day_schedule_time = get_average_time(schedule_time.day_schedule_time, day_vaild_order_count)
    # noon_schedule_time = get_average_time(schedule_time.noon_schedule_time, noon_vaild_order_count)
    # night_schedule_time = get_average_time(schedule_time.night_schedule_time, night_vaild_order_count)
    
    # print(f'{day_schedule_time} {noon_schedule_time} {night_schedule_time}')
        
if __name__ == "__main__":
    main()
    # re = get_average_time(4109023,1257)
    # print(re)