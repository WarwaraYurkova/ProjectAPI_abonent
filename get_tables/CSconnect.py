from fastapi import FastAPI
import pyodbc
import fdb
import pandas as pd
import sqlalchemy as sa

connection_url_fdb = sa.engine.URL.create(
  "firebird",
  username="SYSDBA",
  password="305",
  host="192.168.50.7/3305",
  database=r"D:\Etalon\Russia\RyazanGKH\KVCRegion\ABONENT_3.FDB")

#connection_url_fdb = sa.engine.URL.create("firebird",
                    # username='SYSDBA',
                     #password='masterkey',
                    # host='localhost',
                    # database=r'C:/Users/user/Desktop/API_DB/ABONENT_3.FDB')
engine = sa.create_engine(connection_url_fdb)

engin_MS = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                      "Server=192.168.51.11,1433;"
                      "Database=PaySystemAlpha;"
                      "trusted_connection=no;"
                      "uid=UserAlpha;"
                      "pwd=ALSUtCF%utnWJk6Oa~RI?#q0v!;")
#engin_MS = pyodbc.connect(connection_url_ms)
#engine_MS = sa.create_engine(connection_url_ms)

def Connect_FDB():
    try:
        fdb.load_api('C:/Program Files/Firebird/Firebird_3_0/fbclient.dll')
        print('БД подключена')
        return engine
    except fdb.fbcore.DatabaseError as details:
            errorMsg = "Ошибка: Невозможно подключиться к БД!\nПожалуйста, выберете другой файл для " \
                                        "подключения.\n\nПодробности\n" + str(details).replace("\\n", "\n") + "\n"
            print(errorMsg)
            return False
    except fdb.fbcore.ProgrammingError as details:
            errorMsg = "Ошибка: Неверное значение парамтров!\nПожалуйста, проверьте строку " \
                                                "подключения.\nПодробности: " + str(details) + "\n"
            print(errorMsg)
            return False
    except Exception as errorMsg:
            print("Ошибка: " + str(errorMsg))
            input("Нажмите ENTER чтобы закрыть окно.")
    return -1

Connect_FDB()
