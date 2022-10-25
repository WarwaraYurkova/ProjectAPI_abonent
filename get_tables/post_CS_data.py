from CSconnect import engine
import pandas as pd
from address import address


def general_info_abonent_telephones(P_LSHET):
    # Согласие на обработку ПД
    # Рассылка по электронной почте
    df_general_inf = pd.read_sql("execute block as "
                                 "declare variable USERCD integer; "
                                 "declare variable LSHET varchar(10);"
                                 "declare variable DOCCD integer; "
                                 "declare variable AGREEMENTPERSONALINFO smallint;  /*0,1*/ "
                                 "declare variable CHANGE_DATA date;  /*Дата изменения*/ "
                                 "begin"
                                 "select gen_id(documents_gen,1) from rdb$database into :DOCCD; "
                                 "insert into documents (documentcd,registerusercd,doctypeid,docname,factdocumentdate,reasonid) "
                                 " values (:DOCCD,:USERCD,1,'Согласие на обработку ПД (Битрикс 24)',:CHANGE_DATA, 28);"
                                 "update abonents set AGREEMENTPERSONALINFO = :AGREEMENTPERSONALINFO where lshet = :LSHET; "
                                 "end "
                                 "execute block as "
                                 "declare variable LSHET varchar(10); "
                                 "declare variable DOCCD integer; "
                                 "declare variable USERCD integer; "
                                 "declare variable :CONSENT_TO_MAIL smallint; /*согласие на рассылку 0,1*/ "
                                 "declare variable CHANGE_DATA date; /*Дата изменения*/ "
                                 "begin "
                                 "select gen_id(documents_gen,1) from rdb$database into :DOCCD; "
                                 "insert into documents (documentcd,registerusercd,doctypeid,docname,factdocumentdate,reasonid) "
                                 "values (:DOCCD,:USERCD,8,'Рассылка по электронной почте (Битрикс 24)',:CHANGE_DATA, 6); "
                                 "update or insert into abonentadditionalchars (additionalcharcd, lshet, significance, changedocumentcd) "
                                 "values (1620131,:LSHET,:CONSENT_TO_MAIL,:DOCCD); "
                                 "end ", engine)
