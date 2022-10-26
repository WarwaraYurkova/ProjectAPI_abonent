import datetime
import sqlalchemy
from sqlalchemy import create_engine
import fdb

connection_url = fdb.connect(
    user='SYSDBA',
    password='masterkey',
    host='localhost',
    database=r'C:/Users/user/Desktop/API_DB/ABONENT_3.FDB')
cur = connection_url.cursor()


def post_general_info_abonent(DOCCD, USERCD, CHANGE_DATA, AGREEMENTPERSONALINFO, LSHET, CONSENT_TO_MAIL):
    # Согласие на обработку ПД
    general_inf_agreement = cur.execute("execute block as "
                                        "declare variable USERCD integer; "
                                        "declare variable LSHET varchar(10);"
                                        "declare variable DOCCD integer; "
                                        "declare variable AGREEMENTPERSONALINFO smallint;  /*0,1*/ "
                                        "declare variable CHANGE_DATA date;  /*Дата изменения*/ "
                                        "begin"
                                        f"select gen_id(documents_gen,1) from rdb$database into {DOCCD}; "
                                        "insert into documents (documentcd,registerusercd,doctypeid,docname,factdocumentdate,reasonid) "
                                        f" values ({DOCCD},{USERCD},1,'Согласие на обработку ПД (Битрикс 24)',{CHANGE_DATA}, 28);"
                                        f"update abonents set AGREEMENTPERSONALINFO = {AGREEMENTPERSONALINFO} where lshet = {LSHET}; "
                                        "end ")
    engine.execute(general_inf_agreement).all()
    # Рассылка по электронной почте
    general_inf_consent_mail = pd.read_sql("execute block as "
                                           "declare variable LSHET varchar(10); "
                                           "declare variable DOCCD integer; "
                                           "declare variable USERCD integer; "
                                           f"declare variable {CONSENT_TO_MAIL} smallint; /*согласие на рассылку 0,1*/ "
                                           "declare variable CHANGE_DATA date; /*Дата изменения*/ "
                                           "begin "
                                           f"select gen_id(documents_gen,1) from rdb$database into {DOCCD}; "
                                           "insert into documents (documentcd,registerusercd,doctypeid,docname,factdocumentdate,reasonid) "
                                           f"values ({DOCCD},{USERCD},8,'Рассылка по электронной почте (Битрикс 24)',{CHANGE_DATA}, 6); "
                                           "update or insert into abonentadditionalchars (additionalcharcd, lshet, significance, changedocumentcd) "
                                           f"values (1620131,{LSHET},{CONSENT_TO_MAIL},{DOCCD}); "
                                           "end ", engine)
    engine.execute(general_inf_consent_mail).all()
    engine.commit()


def post_telephones_abonent(LSHET, PHONETYPE,PHONENUMBER, SOURCEID, OWNERTYPEID,USERCD,CHANGE_DATA):
    date_right = datetime.datetime.strptime(CHANGE_DATA, "%d.%m.%Y %H:%M")
    # Контактные телефоны
    table_telephones = cur.execute("execute block as "
                                   "declare variable LSHET varchar(10); "
                                   "declare variable USERCD integer; "
                                   "declare variable PHONETYPE integer; "
                                   "declare variable PHONENUMBER varchar(100); "
                                   "declare variable OWNERTYPEID Integer; "
                                   "declare variable SOURCEID integer; "
                                   "declare variable CHANGE_DATA date; "
                                   "begin "
                                   "insert into abonentphones (id,lshet,phonetypeid,phonenumber,commdate,ownertypeid,sourceid,usercd,ts)"
                                   f"values (gen_id(abonentphones_g,1),'{LSHET}','{PHONETYPE}','{PHONENUMBER}','{date_right}','{OWNERTYPEID}',"
                                   f"'{SOURCEID}','{USERCD}','{date_right}'); "
                                   "end")
    connection_url.commit()


def post_email(LSHET, EMAILTYPEID, EMAIL, CHANGE_DATA, OWNERTYPEID, SOURCEID, USERCD):
    date_right_chd = datetime.datetime.strptime(CHANGE_DATA, "%d.%m.%Y %H:%M")
    table_email = cur.execute("execute block as "
                              "declare variable LSHET varchar(10); "
                              "declare variable USERCD integer; "
                              "declare variable EMAIL string; "
                              "declare variable PHONETYPE integer; "
                              "declare variable OWNERTYPEID Integer; "
                              "declare variable SOURCEID integer; "
                              "declare variable CHANGE_DATA date; "
                              "begin "
                              "insert into ABONENTSMAIL (ID, LSHET, EMAILTYPEID, EMAIL, COMMDATE, OWNERTYPEID, SOURCEID, USERCD, TS) "
                              f"values (gen_id(GEN_ABONENTSMAIL_ID,1), "
                              f"{LSHET}, {EMAILTYPEID}, {EMAIL}, {CHANGE_DATA}, {OWNERTYPEID}, {SOURCEID}, {USERCD}, {CHANGE_DATA});"
                              "end")
    connection_url.commit()
