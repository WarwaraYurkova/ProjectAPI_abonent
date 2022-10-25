from CSconnect import engine
import pandas as pd
import sqlalchemy
from address import address


def general_info_abonent_telephones(P_LSHET):
    # Общая информация о ЛС, Контактные телефоны, Электронная почта
    df_general_inf = pd.read_sql(
        "select ABONENTS.LSHET, EXTORGACCOUNTS.EXTLSHET, extorgspr.extorgnm, "
        "ABONENTS.FIO, ABONENTS.NAME, ABONENTS.SECOND_NAME, "
        "ABONENTS.AGREEMENTPERSONALINFO , INFORMATIONOWNERS.OWNERNAME , ABONENTS.PCLOGIN,"
        "ABONENTSCONTRACT.DOCUMENTCD, ABONENTSCONTRACT.STARTDATE "
        "from abonents "
        "LEFT JOIN EXTORGACCOUNTS ON ABONENTS.LSHET=EXTORGACCOUNTS.LSHET "
        "LEFT JOIN INFORMATIONOWNERS ON ABONENTS.OWNERID = INFORMATIONOWNERS.OWNERID "
        "LEFT JOIN ABONENTSCONTRACT ON ABONENTS.LSHET=ABONENTSCONTRACT.LSHET "
        "join extorgspr on extorgspr.extorgcd=extorgaccounts.extorgcd "
        f" where ABONENTS.LSHET ={P_LSHET}", engine)
    df_general_info = pd.DataFrame(df_general_inf)
    df_general_info["extlshets"] = (
            df_general_info["extlshet"].apply(str) + ' (' + df_general_info["extorgnm"].apply(str) + ')')
    df_general_info.set_index(['lshet', 'extorgnm'])
    df_general_info.explode('extlshets')

    if df_general_info.empty:
        return f"Лицевого счета {P_LSHET} не существует"

    df_general_info["abonentFIO"] = (
            df_general_info["fio"].apply(str) + ' ' + df_general_info["name"].apply(str) + ' '
            + df_general_info["second_name"].apply(str))

    # df_general_info["documentcd"] = df_general_info["documentcd"].fillna("Не указан", inplace=True)
    df_general_info["pclogin"] = \
        df_general_info["pclogin"].apply(pd.to_numeric, errors='coerce').fillna("", inplace=True)
    df_general_info["startdate"] = df_general_info["startdate"].apply(pd.to_datetime, errors='coerce').fillna("",
                                                                                                              inplace=True)

    a = address(P_LSHET)
    a = pd.DataFrame(a)
    a = a.set_index('lshet')
    df_general_infos = df_general_info.merge(a[['address']], on='lshet')
    df_general_infos = df_general_infos.reindex(columns=["lshet", "extlshets", "abonentFIO", "address",
                                                         "agreementpersonalinfo", "ownername", "pclogin", "documentcd",
                                                         "startdate"])

    # Контактные телефоны
    df_telephones = pd.read_sql("select lshet, phonetypeid, phonenumber, commdate, ownertypeid, sourceid "
                                "from abonentphones", engine)
    df_telephon = pd.DataFrame(df_telephones)
    df_telephon["commdate"] = df_telephon["commdate"].apply(lambda x: x.date()).fillna("", inplace=True)
    numeric_telephones = ["phonetypeid", "phonenumber", "ownertypeid", "sourceid"]
    df_telephon[numeric_telephones] = df_telephon[numeric_telephones].apply(pd.to_numeric, errors='coerce').fillna("",
                                                                                                                   inplace=True)
    df_telephon = df_telephon.reindex(
        columns=["lshet", "phonetypeid", "phonenumber", "commdate", "ownertypeid", "sourceid"])

    info_telephon = df_general_infos.merge(df_telephon, how='left', on='lshet')
    abonents_telephone_mail = info_telephon.to_dict('records')
    # info_telephon=info_telephon.to_dict('records')
    # eturn info_telephon
    # return abonents_telephone_mail
    print(df_general_info)


def email(P_LSHET):
    # Электронная почта
    table_mail = pd.read_sql(
        "select am.lshet, am.emailtypeid, am.email, am.commdate, am.ownertypeid, am.sourceid "
        "from abonentsmail am "
        f"where am.lshet='{P_LSHET}'", engine)
    df_mail = pd.DataFrame(table_mail)
    if df_mail.empty:
        return f"По л/c {P_LSHET} запись об электронной почте отсутствует"
    df_mail["commdate"] = df_mail["commdate"].apply(lambda x: x.date())
    # columns_mail = ["emailtypeid", "email", "commdate", "ownertypeid", "sourceid"]
    # df_mail[columns_mail] = (df_mail[columns_mail]).apply(
    # lambda x: x.date() if not pd.isnull(x) else "")
    df_mail = df_mail.to_dict('records')
    return df_mail


def accrual_and_payment_history(P_LSHET):
    # История начислений и оплаты
    table_a_p_history = pd.read_sql(
        "select a.lshet,p.fyear, p.fmonth, p.peninachislsumma "
        f"from penisumma p  join  abonents a using(lshet) where a.lshet='{P_LSHET}'", engine)
    df_a_p_history = pd.DataFrame(table_a_p_history)
    if df_a_p_history.empty:
        return f"По л/c {P_LSHET} запись по истории начислений и оплаты отсутствует"
    a_p_history = df_a_p_history.to_dict('records')
    return a_p_history


def citizens_and_benefits(P_LSHET):
    # Граждане и льготы
    df_citizen = pd.read_sql("select abonents.lshet,cityzens.cityzen_id, cityzens.ctzfio,cityzens.ctzname, "
                             "cityzens.ctzparentname from cityzens "
                             f"join abonents on abonents.lshet=cityzens.lshet where abonents.lshet={P_LSHET}", engine)
    df_citizens_benefits = (engine.execute(df_citizen)).all()
    df_citizens_benefits = pd.DataFrame(df_citizens_benefits)
    if df_citizens_benefits.empty:
        return f"Записи по ЛС {P_LSHET} не существует"
    df_citizens_benefits["sitizenFIO"] = (
            df_citizens_benefits["ctzfio"].apply(str) + ' ' + df_citizens_benefits["ctzname"].apply(str) + ' ' +
            df_citizens_benefits["ctzparentname"].apply(str))
    columnsDEL = ["ctzfio", "ctzname", "ctzparentname"]
    df_citizens_benefits.drop(columnsDEL, inplace=True, axis=1)

    df_citizens_benefit = df_citizens_benefits.reindex(columns=["lshet", "cityzen_id", "sitizenFIO"])

    df_citizens_statuses = pd.read_sql(
        "select abonents.lshet, citizenstates.citizenstatename, CITIZENSTATUSES.statusdate "
        "FROM citizenstatuses "
        "LEFT join cityzens on cityzens.cityzen_id=citizenstatuses.cityzen_id "
        "LEFT join abonents on abonents.lshet=cityzens.lshet "
        "left JOIN CITIZENSTATES ON CITIZENSTATES.citizenstateid= CITIZENSTATUSES.citizenstateid", engine)
    df_citizens_statuses = (engine.execute(df_citizens_statuses)).all()
    df_citizens_statuses = pd.DataFrame(df_citizens_statuses)
    if df_citizens_statuses.empty:
        return "По данному лицевому счету запись о гражданах и льготах отсутствует"
    df_citizens_benefit = df_citizens_benefit.merge(df_citizens_statuses, on="lshet")
    df_citizens_benefit.drop(["cityzen_id"], inplace=True, axis=1)
    df_citizens_benefit = df_citizens_benefit.tail(1)
    df_citizens_benefit = df_citizens_benefit.to_dict('records')
    return (df_citizens_benefit)


def consumption(P_LSHET):
    # Потребление
    table_consumption = pd.read_sql(
        "select Lcharsabonentlist.lshet, lcharslist.name, logicvalues.logicsignificance "
        "from lcharsabonentlist "
        "left join lcharslist on lcharslist.kod=lcharsabonentlist.kodlcharslist "
        "left join logicvalues on logicvalues.significance=lcharsabonentlist.significance "
        "and logicvalues.kod=lcharsabonentlist.kodlcharslist "
        f"where lcharslist.kod in (53,52,34,6,5,2,30,22,21,12,11,96,10009) and Lcharsabonentlist.lshet={P_LSHET}",
        engine)
    df_consumption = (engine.execute(table_consumption)).all()
    df_consumption = pd.DataFrame(df_consumption)
    if df_consumption.empty:
        return f"По л/c {P_LSHET} запись об оборудовании отсутствует"
    df_consumption = df_consumption.to_dict('records')
    return (df_consumption)


def equipment(P_LSHET):
    # Оборудование
    table_equipment = pd.read_sql(
        "select (cntr.name||' (модель '||cntr.model||', '||' код '|| cntr.kod||')'), rscntr.name, "
        "parentequipment.serialnumber,rscntr.setupdate, rscntr.lastpprdate, rscntr.dateppr, "
        "cntr.digitcount,  periodppr.name, eqsl.statusvalue,/*equipmentadditionalchars,*/ "
        "counterindication.previousindication,/*abonentadditionalchars,*/ counterindication.indicationvalue "
        "/*equipmentadditionalchars,*/"
        "/*equipmentadditionalchars,*/ "
        "from counterstypes cntr "
        "join resourcecounters rscntr on cntr.kod=rscntr.kodcounterstypes "
        "join eqstatuses eqstt on eqstt.equipmentid=cntr.equipmenttypeid "
        "join eqstatuslist eqsl on eqstt.statuscd=eqsl.statuscd "
        "join parentequipment on parentequipment.equipmentid=eqstt.equipmentid "
        "join periodppr on periodppr.kod=rscntr.kodperiodppr "
        "join equipmentadditionalchars eqadc on eqadc.equipmentid=eqstt.equipmentid "
        "join additionalchars adc on adc.additionalcharcd=eqadc.additionalcharcd "
        "join counterindication on counterindication.kod=rscntr.kod", engine)


def lawsuits_claims(P_LSHET):  # посмотреть запрос
    # Иски, претензии
    df_info_lawsuits = pd.read_sql(
        "select abonents.lshet,lawsuits.suitstatusdate, documenttypes.doctypename,documents.inputdate,"
        "documents.outputdate, documents.documentcd, "
        "lawsuitsstatushistory.suitstatusdate,avaliablesuitstates.suitstatusname "
        "from documents "
        "left join abonents on abonents.adddocumentcd=documents.documentcd "
        "join lawsuits on lawsuits.suitstatusdocumentcd=documents.documentcd "
        "join documenttypes on documents.doctypeid=documenttypes.doctypeid "
        "join lawsuitsstatushistory on lawsuitsstatushistory.suitstatusdocumentcd= documents.documentcd "
        "join avaliablesuitstates on avaliablesuitstates.suitstatuscd=lawsuits.suitstatuscd where "
        f"abonents.lshet={P_LSHET}", engine)
    df_info_p = (engine.execute(df_info_lawsuits)).all()
    df_info_p = pd.DataFrame(df_info_p)
    if df_info_p.empty:
        return "По данному лицевому счету запись об исках отсутствует"
    pr_dates = ["inputdate", "outputdate"]
    df_info_p[pr_dates] = df_info_p[pr_dates].apply(pd.to_datetime, errors='coerce')
    df_info_p["condition"] = df_info_p["suitstatusdate"].apply(str) + ': ' + df_info_p["suitstatusname"].apply(str)
    df_info_pr = df_info_p.reindex(
        columns=["lshet", "documencd", "doctypename", "suitstatusdate", "inputdate", "outputdate", "condition"])
    df_info_pr = df_info_pr.to_dict('records')
    return (df_info_pr)


def house_characteristics(P_LSHET):
    # Характеристики
    df_characters = pd.read_sql(
        "select abonents.lshet, ccharslist.name, ccharsabonentlist.significance,ccharsabonentlist.abonentcchardate "
        "from ccharsabonentlist "
        "join abonents on abonents.lshet=ccharsabonentlist.lshet "
        "left join ccharslist on ccharslist.kod = ccharsabonentlist.kodccharslist where ccharslist.kod in (1, 2, 3, "
        f"11, 12, 13, 23, 22, 26) and abonents.lshet=:{P_LSHET}", engine)
    df_characters = (engine.execute(df_characters, PLSHET=P_LSHET)).all()
    df_characters = pd.DataFrame(df_characters)
    if df_characters.empty:
        return f"По л/с {P_LSHET} запись о характеристиках отсутствует"
    df_characters["abonentcchardate"] = df_characters["abonentcchardate"].apply(lambda x: x.date())
    # df_characters["characters_info"] = df_characters["name"].apply(str) + ': ' + df_characters[
    # "significance"].apply(str)
    df_charactr = df_characters.reindex(columns=["lshet", "characters_info", "abonentcchardate"])
    df_charactr = df_charactr.to_dict('records')
    return df_charactr


def additional_house_ch(P_LSHET):
    # Дополнительные сведения о доме
    df_additional_ch = pd.read_sql(
        "select abonents.lshet, ccharslist.name, ccharshouselist.significance, ccharshouselist.housecchardate "
        "from ccharshouselist "
        "inner join ccharslist on ccharslist.kod = ccharshouselist.kod "
        "left join houses on houses.housecd=ccharshouselist.housecd "
        "left join abonents on abonents.housecd=houses.housecd "
        f"where ccharslist.kod in (32011, 32012, 31003, 206004) and abonents.lshet=:{P_LSHET}", engine)
    df_additional_ch = (engine.execute(df_additional_ch)).all()
    df_additional_ch = pd.DataFrame(df_additional_ch)
    if df_additional_ch.empty:
        return f"По л/с {P_LSHET} запись о дополнительных характеристиках отсутствует"
    df_additional_ch["housecchardate"] = df_additional_ch["housecchardate"].apply(lambda x: x.date())
    df_additional_ch["additional_ch_info"] = df_additional_ch["name"].apply(str) + ': ' + df_additional_ch[
        "significance"].apply(str)
    df_additional_ch = df_additional_ch.reindex(columns=["lshet", "additional_ch_info", "housecchardate"])
    df_additional_ch = df_additional_ch.to_dict('records')
    print(df_additional_ch.columns)


def consumption_parameters(P_LSHET):
    # Параметры потребления
    cons_param_table = pd.read_sql(
        "select abonents.lshet, houses.housecd, lcharslist.name, "
        "logicvalues.logicsignificance,lcharshouselist.houselchardate "
        "from lcharshouselist "
        "left join houses on houses.housecd=lcharshouselist.housecd "
        "left join abonents on abonents.housecd=houses.housecd "
        "left join lcharslist on lcharslist.kod=lcharshouselist.lcharshouselistid "
        "left join logicvalues on logicvalues.significance=lcharshouselist.significance and "
        "logicvalues.kod=lcharshouselist.lcharshouselistid "
        f"where lcharslist.kod in (1,37,99,10009,12,21,44,32,62990,22,30,127,126,68,69,70) and abonents.lshet=:{P_LSHET}",
        engine)
    df_cons_param = (engine.execute(cons_param_table)).all()
    df_cons_param = pd.DataFrame(df_cons_param)
    if df_cons_param.empty:
        return f"По л/с {P_LSHET} запись о параметрах потребления отсутствует"
    return df_cons_param
