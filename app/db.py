import pymysql
from .utils import Singleton
pymysql.install_as_MySQLdb()

class DbConnection:
    def __init__(self, host, port, user, password, db):
        self.con = pymysql.connect(host=host, port=int(port), user=user,
                                   password=password, db=db, cursorclass=pymysql.cursors.DictCursor)

    def query(self, sql):
        try:
            with self.con.cursor() as cur:
                cur.execute(sql)
                result = cur.fetchall()
                return result
        except Exception as e:
            print(f'[!] connection error! {e}')
            self.con.close()
    
if __name__ == "__main__":
    db1 = DbConnection(host='59.63.222.206', port=33666, user='test', password='123456', db='fns')
    db2 = DbConnection(host='59.63.222.206', port=33666, user='test', password='123456', db='fns')
    # sql="SELECT * FROM t_waybill_11 WHERE finish_time>='2018-11-14 00:00:00' and finish_time<='2018-11-14 23:59:59' and city_id = '1' and ele_point_id = '17417939' and standard_type = '44' and waybill_type = '普通单'"
    # result = db.query(sql)
    # import pandas as pd
    # df = pd.DataFrame(result)
    # print(result)
    print(db1)
    print(db2)
    