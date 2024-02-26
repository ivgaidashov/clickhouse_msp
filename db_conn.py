import clickhouse_driver
import cx_Oracle
from datetime import datetime
import time
from conf import ch_address, ch_db, ch_port, ch_user, ch_password, oracle_password, oracle_username, oracle_database, oracle_ip
from utils import error_handler

class ClickhouseDB(object):
    connection = None

    def __init__(self):
        while ClickhouseDB.connection is None:
            try:
                error_handler('I', 'Подключение к Clickhouse')
                ClickhouseDB.connection = clickhouse_driver.Client(ch_address, user=ch_user, password=ch_password, port=ch_port,
                                                 database=ch_db, settings={'use_numpy': True})
                if ClickhouseDB.connection is None:
                    time.sleep(5)
            except Exception as error:
                error_handler('E', 'Ошибка подключения к Clickhouse: ' + str(error))

    def check_duplicate_zip(self, zip_title):
        returned_rows = None
        try:
            error_handler('I', f'Проверка загрузки архива {zip_title} ранее')
            rows = ClickhouseDB.connection.execute('SELECT Title from msp_archives where Title = %(parameter)s', {'parameter': zip_title})
            returned_rows = len(rows)
        except Exception as error:
            error_handler('E', 'Ошибка сохранения данных: ' + str(error))

        if returned_rows >= 1:
            error_handler('E', f'Архив с таким именем уже загружен {returned_rows} раз(а). Процесс завершён')
            raise

    def get_msp_status(self, icusnum, inn):
        try:
            error_handler('I', f'Получение МСП статуса для клиента {icusnum} ')
            rows = ClickhouseDB.connection.execute('''
                                                    select if (MSPDate = makeDate(1970,01,01), addMonths(prevDate,1), MSPDate), MSPType from (
                                                    select m.MSPType,
                                                           lagInFrame(MSPType) OVER (ORDER BY d.DataDate ASC ROWS 1 PRECEDING) AS prevType,
                                                           if (m.MSPDate = makeDate(2016,08,10), makeDate(2016,08,01), m.MSPDate) as MSPDate,
                                                           lagInFrame(MSPDate) OVER (ORDER BY d.DataDate ASC ROWS 1 PRECEDING ) AS prevDate
                                                                      from (select MSPType, MSPDate from tkpb.msp_registry where INN = %(parameter)s) m  
                                                    right join tkpb.msp_archives d on m.MSPDate=d.DataDate) where MSPType <> prevType
                                                    '''
                                                   , {'parameter': inn})
            msp_statuses = []
            if rows:
                for row in rows:
                    msptype = None

                    if row[1] == 0:
                        msptype = 'Заемщик не является субъектом малого и среднего предпринимательства'
                    elif row[1] == 1:
                        msptype = 'Микробизнес'
                    elif row[1] == 2:
                        msptype = 'Малый'
                    elif row[1] == 3:
                        msptype = 'Средний'
                    else:
                        msptype = row[1]

                    print(msptype)
                    res = {'ICUSNUM': icusnum, 'MSPDATE': row[0], 'MSPTYPE': msptype}
                    msp_statuses.append(res)
            else:
                res = {'ICUSNUM': icusnum, 'MSPDATE': datetime.today().date(), 'MSPTYPE': 'Не включался в МСП'}
                msp_statuses.append(res)
            return msp_statuses
        except Exception as error:
            error_handler('E', f'Клиент {icusnum}: Ошибка получения статуса МСП: ' + str(error))
    def insert_df(self, table, df):
        try:
            if len(df) > 0:
                error_handler('I', f'Сохранение данных: {table}')
                columns = ', '.join(df.keys())
                query = f'INSERT INTO {table} ({columns}) VALUES '
                rows = ClickhouseDB.connection.insert_dataframe(query, df)
                error_handler('I', f'Отправлено строк: {rows}')
            else:
                error_handler('W', f'Нет данных для сохранения для {table}')
        except Exception as error:
            error_handler('E', 'Ошибка сохранения данных: ' + str(error))
            raise

    def save_zip(self, zip, mspdate):
        try:
            error_handler('I', f'Сохранение архива {zip} в истории загрузок')
            ClickhouseDB.connection.execute('insert into msp_archives values (%(parameter1)s, %(parameter2)s, now())', {'parameter1': zip, 'parameter2': mspdate})
        except Exception as error:
            error_handler('E', f'Ошибка сохранения архива {zip} в истории загрузок: ' + str(error))

    def close(self):
        error_handler('I', 'Закрытие соединения с Clickhouse')
        ClickhouseDB.connection.disconnect()

class OracleDB(object):
    connection = None

    def __init__(self):
        while OracleDB.connection is None:
            try:
                error_handler('I', 'Подключение к Oracle')
                dsn_tns = cx_Oracle.makedsn(oracle_ip, '1521', service_name=oracle_database)
                OracleDB.connection = cx_Oracle.connect(user=oracle_username, password=oracle_password, dsn=dsn_tns)
                if OracleDB.connection is None:
                    time.sleep(5)
            except cx_Oracle.Error as error:
                error_handler('E', f'Ошибка подключения к Oracle: {str(error)}')

    def execute_many(self, data):
        error_handler('I', "Сохранение результатов в БД")
        cols = ','.join(list(data[0].keys()))
        params = ','.join(':' + str(k) for k in list(data[0].keys()))
        statement = f"insert into gis_msp_ch ({cols}) values ({params})"

        for one in data:
            print(one)


        with self.connection.cursor() as cursor:
            try:
                cursor.executemany(statement, data, batcherrors=True)
                self.connection.commit()
                for error in cursor.getbatcherrors():
                    error_handler('E',f"БД: ошибка загрузки: \n{error.message}. \n{data[error.offset]}")
            except Exception as e:
                error_handler('E',  'Ошибка загрузки в БД ' + str(e))
    def close(self):
        error_handler('I', 'Закрытие соединения к Oracle')
        OracleDB.connection.close()