from CSconnect import engine
import pandas as pd
from address import address


def general_info_abonent_telephones(P_LSHET):
    # Общая информация о ЛС, Контактные телефоны, Электронная почта
    df_general_inf = pd.read_sql(
        "select dop_t1.*,abonents.note,ABONENTS.FIO, ABONENTS.NAME, ABONENTS.SECOND_NAME, "
        "ABONENTS.AGREEMENTPERSONALINFO , INFORMATIONOWNERS.OWNERNAME , ABONENTS.PCLOGIN, "
        "dop_t2.documentscds, ABONENTSCONTRACT.STARTDATE "
        "from (select lshet,list(dop_t.extlshets) as lshets from (select ABONENTS.LSHET, "
        "(EXTORGACCOUNTS.EXTLSHET||' ('||extorgspr.extorgnm||')') as extlshets "
        "from abonents "
        "LEFT JOIN EXTORGACCOUNTS ON ABONENTS.LSHET=EXTORGACCOUNTS.LSHET "
        f"join extorgspr on extorgspr.extorgcd=extorgaccounts.extorgcd where abonents.lshet={P_LSHET}) "
        "as dop_t group by lshet)as dop_t1 "
        "join (select abonents.lshet,list(ABONENTSCONTRACT.DOCUMENTCD) as documentscds "
        "from abonents "
        f"join ABONENTSCONTRACT ON ABONENTS.LSHET=ABONENTSCONTRACT.LSHET where abonents.lshet={P_LSHET}  group by lshet) "
        "as dop_t2 on dop_t2.lshet=dop_t1.lshet "
        "join abonents on dop_t1.lshet=abonents.lshet "
        "LEFT JOIN INFORMATIONOWNERS ON ABONENTS.OWNERID = INFORMATIONOWNERS.OWNERID "
        "join ABONENTSCONTRACT ON ABONENTS.LSHET=ABONENTSCONTRACT.LSHET", engine)
    df_general_info = pd.DataFrame(df_general_inf)

    if df_general_info.empty:
        return f"Лицевого счета {P_LSHET} не существует"

    df_general_info["abonentFIO"] = (
            df_general_info["fio"].apply(str) + ' ' + df_general_info["name"].apply(str) + ' '
            + df_general_info["second_name"].apply(str))
    df_general_info["note"] = df_general_info["note"].apply(str)
    df_general_info["documentscds"] = df_general_info["documentscds"].fillna("Не указан", inplace=True)
    df_general_info["pclogin"] = \
        df_general_info["pclogin"].apply(pd.to_numeric, errors='coerce').fillna("", inplace=True)
    df_general_info["startdate"] = df_general_info["startdate"].apply(pd.to_datetime, errors='coerce').fillna("",
                                                                                                              inplace=True)

    a = address(P_LSHET)
    a = pd.DataFrame(a)
    a = a.set_index('lshet')
    df_general_infos = df_general_info.merge(a[['address']], on='lshet')
    #df_general_infos = df_general_infos.reindex(columns=["lshet", "extlshets", "abonentFIO", "address",
                                                         #"agreementpersonalinfo", "ownername", "pclogin", "documentcd",
                                                         #"startdate"])

    # Контактные телефоны
    df_telephones = pd.read_sql(
        "select abp.lshet, abp.phonetypeid, abp.phonenumber, abp.commdate, owt.description as ownertype,"
        " ins.description as sourcetype "
        "from abonentphones abp "
        "join ownertypes owt on owt.id=abp.ownertypeid "
        f"join infosources ins on ins.id=abp.sourceid where abp.lshet={P_LSHET}", engine)
    df_telephon = pd.DataFrame(df_telephones)
    df_telephon["commdate"] = df_telephon["commdate"].apply(lambda x: x.date())
    numeric_telephones = ["phonetypeid", "phonenumber"]
    df_telephon[numeric_telephones] = df_telephon[numeric_telephones].apply(pd.to_numeric, errors='coerce')
    info_telephon = df_general_infos.merge(df_telephon, how='left', on='lshet')
    #abonents_telephone_mail = info_telephon.to_dict('records')
    info_telephone=info_telephon.to_dict('records')
    return info_telephone

def email(P_LSHET):
    # Электронная почта
    table_mail = pd.read_sql(
        "select am.lshet, emt.description as mailtype, am.email, am.commdate, owt.description as ownertype, ins.description as sourcetype "
        "from abonentsmail am "
        "join emailtypes emt on emt.emailtypecd=am.emailtypeid "
        "join ownertypes owt on owt.id=am.ownertypeid "
        "join infosources ins on ins.id=am.sourceid  "
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
    table_citizen = pd.read_sql("select abonents.lshet,cityzens.cityzen_id, cityzens.ctzfio,cityzens.ctzname, "
                                "cityzens.ctzparentname from cityzens "
                                f"join abonents on abonents.lshet=cityzens.lshet where abonents.lshet={P_LSHET}",
                                engine)
    df_citizens_benefits = pd.DataFrame(table_citizen)
    if df_citizens_benefits.empty:
        return f"Записи по ЛС {P_LSHET} не существует"
    df_citizens_benefits["sitizenFIO"] = (
            df_citizens_benefits["ctzfio"].apply(str) + ' ' + df_citizens_benefits["ctzname"].apply(str) + ' ' +
            df_citizens_benefits["ctzparentname"].apply(str))
    columnsDEL = ["ctzfio", "ctzname", "ctzparentname"]
    df_citizens_benefits.drop(columnsDEL, inplace=True, axis=1)

    df_citizens_benefit = df_citizens_benefits.reindex(columns=["lshet", "cityzen_id", "sitizenFIO"])

    table_citizens_statuses = pd.read_sql(
        "select abonents.lshet, citizenstates.citizenstatename, CITIZENSTATUSES.statusdate "
        "FROM citizenstatuses "
        "LEFT join cityzens on cityzens.cityzen_id=citizenstatuses.cityzen_id "
        "LEFT join abonents on abonents.lshet=cityzens.lshet "
        "left JOIN CITIZENSTATES ON CITIZENSTATES.citizenstateid= CITIZENSTATUSES.citizenstateid", engine)
    df_citizens_statuses = pd.DataFrame(table_citizens_statuses)
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
    df_consumption = pd.DataFrame(table_consumption)
    if df_consumption.empty:
        return f"По л/c {P_LSHET} запись о потреблении отсутствует"
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
    df_equipment = pd.DataFrame(table_equipment)
    if df_equipment.empty:
        return f"По л/c {P_LSHET} запись об оборудовании отсутствует"
    df_consumption = df_equipment.to_dict('records')
    return (df_consumption)


def lawsuits_claims(P_LSHET):
    # Иски, претензии
    table_info_lawsuits = pd.read_sql(
        "select lawsuits.lshet, documenttypes.doctypename, Lawsuits.suitstatusdate, "
        "('с '||Documents.inputdate||' по '||documents.outputdate) as period, avaliablesuitstates.suitstatusname, "
        "case "
        "when lawsuits.lshet in(select lawsuits.lshet "
        "from lawsuits join documents  on documents.documentcd=lawsuits.suitcd where documents.doctypeid = 62000147) then True "
        "else False "
        "end installment_agreement "
        "from lawsuits join documents  on documents.documentcd=lawsuits.suitcd "
        "join documenttypes on documenttypes.doctypeid=documents.doctypeid "
        "join lawsuitsstatushistory on lawsuitsstatushistory.suitcd=lawsuits.suitcd "
        "join avaliablesuitstates on avaliablesuitstates.suitstatuscd=lawsuits.suitstatuscd"
        f" where lawsuits.lshet={P_LSHET}", engine)
    df_info_p = pd.DataFrame(table_info_lawsuits)
    if df_info_p.empty:
        return "По данному лицевому счету запись об исках отсутствует"
    df_info_p["suitstatusdate"] = df_info_p["suitstatusdate"].apply(str)
    df_info_p["suitstatusdate"].replace('T00:00:00 ', ' ')
    df_info_pr = df_info_p.to_dict('records')
    return (df_info_pr)


def house_characteristics(P_LSHET):
    # Характеристики
    table_characters = pd.read_sql(
        "select abonents.lshet, ccharslist.name, ccharsabonentlist.significance,ccharsabonentlist.abonentcchardate "
        "from ccharsabonentlist "
        "join abonents on abonents.lshet=ccharsabonentlist.lshet "
        "left join ccharslist on ccharslist.kod = ccharsabonentlist.kodccharslist where ccharslist.kod in (1, 2, 3, "
        f"11, 12, 13, 23, 22, 26) and abonents.lshet={P_LSHET}", engine)
    df_characters = pd.DataFrame(table_characters)
    if df_characters.empty:
        return f"По л/с {P_LSHET} запись о характеристиках отсутствует"
    df_characters["abonentcchardate"] = df_characters["abonentcchardate"].apply(lambda x: x.date())
    df_characters["characters_info"] = df_characters["name"].apply(str) + ': ' + df_characters["significance"].apply(
        str)
    df_charactr = df_characters.reindex(columns=["lshet", "characters_info", "abonentcchardate"])
    df_charactr = df_charactr.to_dict('records')
    return df_charactr


def additional_house_ch(P_LSHET):
    # Дополнительные сведения о доме
    table_additional_ch = pd.read_sql(
        "select abonents.lshet, ccharslist.name, ccharshouselist.significance, ccharshouselist.housecchardate "
        "from ccharshouselist "
        "inner join ccharslist on ccharslist.kod = ccharshouselist.kod "
        "left join houses on houses.housecd=ccharshouselist.housecd "
        "left join abonents on abonents.housecd=houses.housecd "
        f"where ccharslist.kod in (32011, 32012,5, 206003, 31003, 205004, 206004) and abonents.lshet={P_LSHET}", engine)
    df_additional_ch = pd.DataFrame(table_additional_ch)
    if df_additional_ch.empty:
        return f"По л/с {P_LSHET} запись о дополнительных характеристиках отсутствует"
    df_additional_ch["housecchardate"] = (df_additional_ch["housecchardate"]).apply(str)
    df_additional_ch["housecchardate"].replace('T00:00:00 ', ' ')
    df_additional_ch["additional_ch_info"] = df_additional_ch["name"].apply(str) + ': ' + df_additional_ch[
        "significance"].apply(str)
    df_additional_ch = df_additional_ch.reindex(columns=["lshet", "additional_ch_info", "housecchardate"])
    df_additional_ch = df_additional_ch.to_dict('records')
    return df_additional_ch


def consumption_parameters(P_LSHET):
    # Параметры потребления
    table_cons_param = pd.read_sql(
        "select abonents.lshet, houses.housecd, lcharslist.name, "
        "logicvalues.logicsignificance,lcharshouselist.houselchardate "
        "from lcharshouselist "
        "left join houses on houses.housecd=lcharshouselist.housecd "
        "left join abonents on abonents.housecd=houses.housecd "
        "left join lcharslist on lcharslist.kod=lcharshouselist.lcharshouselistid "
        "left join logicvalues on logicvalues.significance=lcharshouselist.significance and "
        "logicvalues.kod=lcharshouselist.lcharshouselistid "
        f"where lcharslist.kod in (6,2,5,11,12,21,41,42,46,30,47,34) and abonents.lshet={P_LSHET}",
        engine)
    df_cons_param = pd.DataFrame(table_cons_param)
    if df_cons_param.empty:
        return f"По л/с {P_LSHET} запись о параметрах потребления отсутствует"
    df_cons_param = df_cons_param.to_dict('records')
    return df_cons_param
