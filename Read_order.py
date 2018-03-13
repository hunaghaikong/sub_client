from datetime import datetime
import pymysql
from sub_client import SubPrice


class ComWrite:
    def __init__(self):
        try:
            self.conn = pymysql.connect(host='', user='', password='',
                                        db='', charset='utf8')
            self.cur = self.conn.cursor()
        except Exception as exc:
            raise exc
        try:
            self.sub_hsi = SubPrice('HSIH8')
            self.sub_mhi = SubPrice('MHIH8')
            self.sub_hsi.sub()
            self.sub_mhi.sub()
        except Exception as exc:
            raise exc

        '''import pandas as pd
        a = {'Ticket': 48827778}
        df = pd.DataFrame([[_, nd] for _, nd in a.items()])
        df.to_csv()'''

    def get_price(self):
        '''返回tick报价'''
        hsi_price = self.sub_hsi.get_price()
        mhi_price = self.sub_mhi.get_price()
        tick_price={
            'hsi_ask':hsi_price.Ask,
            'hsi_bid':hsi_price.Bid,
            'mhi_ask':mhi_price.Ask,
            'mhi_bid':mhi_price.Bid
        }
        return tick_price

    def write_csv(self, new_order):
        '''写入CSV文件'''
        tick_price = self.get_price()
        Status = new_order.get('Status')
        if Status == 1:
            with open('transaction_data.csv', 'a') as f:
                f.write(str(datetime.now()).split('.')[0] + ',' + str(new_order['Ticket']) + ',' + str(
                    new_order['OpenTime']) + ',' +
                        str(new_order['OpenPrice']) + ',' + str(Status) + ',' +
                        str(new_order['Type']) + str(tick_price['hsi_ask']) + ',' + str(tick_price['hsi_bid']) + ',' +
                        str(tick_price['mhi_ask']) + ',' + str(tick_price['mhi_bid']) + '\n')
                # p.loc[max(p.index) + 1, :] = d1
        elif Status == 2:
            with open('transaction_data.csv', 'a') as f:
                f.write(str(datetime.now()).split('.')[0] + ',' + str(new_order['Ticket']) + ',' + str(
                    new_order['CloseTime']) + ',' +
                        str(new_order['ClosePrice']) + ',' + str(Status) + ',' +
                        str(new_order['Type']) + str(tick_price['hsi_ask']) + ',' + str(tick_price['hsi_bid']) + ',' +
                        str(tick_price['mhi_ask']) + ',' + str(tick_price['mhi_bid']) + '\n')

    def write_sql(self, new_order):
        '''写入数据库'''
        tick_price = self.get_price()
        Status = new_order.get('Status')
        Type = new_order.get('Type')

        sql = 'INSERT INTO futures_comparison(ticket,tickertime,tickerprice,' \
              'openclose,longshort,HSI_ask,HSI_bid,MHI_ask,MHI_bid) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        if Status == 1:
            operation_time = new_order['OpenTime']
            price = new_order['OpenPrice']

        elif Status == 2:
            operation_time = new_order['CloseTime']
            price = new_order['ClosePrice']

        sql_data = (
            str(new_order['Ticket']), operation_time, price, Status-1, Type,
            tick_price['hsi_ask'], tick_price['hsi_bid'], tick_price['mhi_ask'], tick_price['mhi_bid']
        )
        try:
            self.cur.execute(sql, sql_data)
            self.conn.commit()
        except Exception as exc:
            print(exc)

    def _main(self,new_order):
        try:
            self.write_sql(new_order)
        except Exception as exc:
            print(exc)
        try:
            self.write_csv(new_order)
        except Exception as exc:
            print(exc)

    def close_sql(self):
        try:
            self.conn.close()
        except Exception as exc:
            print(exc)

if __name__=='__main__':
    print(ComWrite().get_price())
