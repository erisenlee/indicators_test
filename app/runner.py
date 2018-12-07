from .utils import ReadConfig
from .db import DbConnection
import pandas as pd
from collections import namedtuple

class Dataset:
    def __init__(self,db,conn=None, query_dict=None):
        if conn is None:
            self.conn = self._get_conn(db)
        if query_dict is None:
            self.query = self._query_string(query_dict)

    def _get_conn(self,db):
        db_config = ReadConfig().get_section(db)
        # print(db_config)
        db = DbConnection(host=db_config['host'], port=db_config['port'],user=db_config['user'],password=db_config['password'],db=db_config['db'])
        return db

    def _get_query_config(self):
        query_config = ReadConfig().get_section('query')
        query_config = {key: value for key, value in query_config.items() if value}
        return query_config

    def _map_config(self, query_config):
        order_from = query_config.get('order_from', None)
        waybill_type = query_config.get('waybill_type', None)

        if not (order_from or waybill_type):
            return query_config

        if order_from:
            if order_from == '饿了么主站':
                query_config['order_from'] = 'ELEME'
            elif order_from == '开放平台':
                query_config['order_from'] = 'KFPT'
            else:
                query_config['order_from'] = 'FEORDER'
        if waybill_type:
            if waybill_type == '普通单':
                query_config['waybill_type'] ='normal'
            else:
                query_config['waybill_type'] = 'booked'
        return query_config

    def _get_sql(self):
        query_config = self._get_query_config()
        time = ReadConfig().get_section('time')
        table = ReadConfig().get_section('table')
        raw_sql = "SELECT * FROM {} WHERE statistical_date>='{}' and statistical_date<='{}' ".format(table['table'],time['begin'],time['finish'])
        query_config = self._map_config(query_config)
        if query_config.get('shipper_name', None):
            query_config['merchant_name'] = query_config['shipper_name']
            del query_config['shipper_name']
        result = []
        for key, value in query_config.items():
            s = "and {} = '{}'".format(key, value)
            result.append(s)
        # print(result)
        raw_sql += ' '.join(result)
        return raw_sql

    def _get_raw_sql(self, pre_sql, query_config):
        result = []
        for key, value in query_config.items():
            s = "and {} = '{}'".format(key, value)
            result.append(s)
        # print(result)
        pre_sql += ' '.join(result)
        return pre_sql



    def _query_string(self, query_dict=None):
        query_config = {}
        if not query_dict:
            query_config =query_dict
        query_config = ReadConfig().get_section('query')
        query_config = {key: value for key, value in query_config.items() if value}
        time = ReadConfig().get_section('time')
        raw_sql = "SELECT * FROM t_waybill_0 WHERE finish_time>='{} 00:00:00' and finish_time<='{} 23:59:59' ".format(time['begin'], time['finish'])
        if not query_config:
            return raw_sql
        raw_sql = self._get_raw_sql(raw_sql, query_config)
        return raw_sql

    def _get_service_indicators(self,table, pre_sql):
        time = ReadConfig().get_section('time')
        pre_sql = pre_sql.format(table,time['begin'],time['finish'])
        query_config = self._get_query_config()
        query_config = self._map_config(query_config)
        peak = query_config.get('peak', None)
        if peak:
            del query_config['peak']
        courier_type = query_config.get('courier_type', None)
        merchant_name = query_config.get('shipper_name', None)

        if courier_type:
            query_config['job_category'] = query_config['courier_type']
            del query_config['courier_type']
        if merchant_name:
            query_config['merchant_name'] = query_config['shipper_name']
            del query_config['shipper_name']
        
        raw_sql = self._get_raw_sql(pre_sql, query_config)
        print(f'####execute {table} sql : {raw_sql}####\n')

        result = self.conn.query(raw_sql)
        if not result:
            print(f'[!]service data is none. {table}')
        count = len(result)
        return count

    def _get_good_order(self):
        pre_sql = "SELECT * FROM {} WHERE star=3 AND is_time_valid = 1 AND FROM_UNIXTIME(created_at/1000)>='{} 00:00:00' AND FROM_UNIXTIME(created_at/1000)<='{} 23:59:59' "
        return self._get_service_indicators('t_evaluate_detail_0',pre_sql)

    def _get_negtive_order(self):
        pre_sql = "SELECT * FROM {} WHERE is_time_valid=1 AND event_status=90 and FROM_UNIXTIME(created_at / 1000 ) >= '{} 00:00:00' AND FROM_UNIXTIME(created_at / 1000 ) <= '{} 23:59:59' "
        return self._get_service_indicators('t_evaluate_bad_detail', pre_sql)

    def _get_feedback(self):
        pre_sql = "SELECT * FROM {} WHERE reason_id NOT IN (250,310,320) AND is_time_valid=1 AND STATUS=90 and FROM_UNIXTIME(created_at/1000) >= '{} 00:00:00' AND FROM_UNIXTIME(created_at/1000) <= '{} 23:59:59' "
        return self._get_service_indicators('t_feedback_detail', pre_sql)

    def _get_violations(self):
        pre_sql = "SELECT * FROM {} WHERE reason_id IN (250,310,320) AND is_time_valid=1 AND STATUS=90 and FROM_UNIXTIME(created_at/1000) >= '{} 00:00:00' AND FROM_UNIXTIME(created_at/1000) <= '{} 23:59:59' "
        return self._get_service_indicators('t_feedback_detail', pre_sql)

    def good_order_dict(self):
        GoodOrder = namedtuple('GoodOrder', ['good_order_count'])
        good_order_count =  GoodOrder(
            good_order_count=self._get_good_order()
        )
        return {'good_order_count':good_order_count}

    def get_service_data(self):
        good_order = self._get_good_order()
        negtive_order = self._get_negtive_order()
        feed_back_order = self._get_feedback()
        violations_order = self._get_violations()
        ServiceIndicators = namedtuple('ServiceIndicators', [
            'good_order',
            'negtive_order',
            'feed_back_order',
            'violations_order',
        ])

        return ServiceIndicators(
            good_order=good_order,
            negtive_order=negtive_order,
            feed_back_order=feed_back_order,
            violations_order=violations_order
        )

    

    def get_assert_data(self):
        sql = self._get_sql()
        print(f'execute sql is {sql}')
        try:
            result = self.conn.query(sql)
        except Exception as e:
            print(f'[!] Get assert data faild.{e} \nExecute sql is \n{sql}')
        # if len(result) > 1:
        #     return pd.DataFrame(result)
        # print(result)
        try:

            return pd.DataFrame(result)
        except Exception as e:
            print(f'[!] Get assert dataframe failed. {e}')


    def get_dataset(self):
        print(f'[+] execute sql is {self.query}')
        result = self.conn.query(self.query)
        # print(result)
        try:
            dataset = pd.DataFrame(result)
            if dataset.empty:
                raise ValueError('[!]get data failed!')
            return dataset
        except Exception as e:
            print(f'[!] read data fail! {e}')
        



if __name__ == "__main__":
    # from . import core
    dataset = Dataset('dbtest').get_dataset()
    print(dataset)
    # print(dataset.get_dataset().shape)
    # i = Indicators(dataset)
    # value = i.get_indicators_value()


            
