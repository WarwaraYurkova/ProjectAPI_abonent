from CSconnect import engin_MS
import pandas as pd


def nofications(P_LSHET):
    # Уведомления
    table_nofications = pd.read_sql("select * from (select distinct LawCases.Account, LawNoticeTypes.Name as "
                                    "NoticeTypeName, "
                                    "case LawNoticeMethodId "
                                    "when 10 then 'E-mail оповещение (внутренний сервис)' "
                                    " when 20 then 'SMS оповещение (API Мегафон)' "
                                    "when 30 then 'Почтовое оповещение (простое письмо)' "
                                    " when 31 then 'Почтовое оповещение (заказное письмо)' "
                                    "end as MethodName,LawNotices.NoticeSumma, LawNotices.RequestDateTime, "
                                    "LawNotices.ResponseDateTime, "
                                    " case LawNoticeStatusId "
                                    " when 0 then 'Новый' "
                                    "when 1 then 'В обработке' "
                                    "when 2 then 'Отправлено' "
                                    "when 3 then 'Прервано' "
                                    "when 4 then 'Доставлено' "
                                    "when 5 then 'Не доставлено' "
                                    "end NoticeStatusName, "
                                    "case LawNoticeParamValue "
                                    "when 'OnlineDelivered' then 'Письмо доставлено в электронном виде' "
                                    "when 'DeliveredToPrint' then 'Письмо доставлено в бумажном виде' "
                                    " end NoticeParamName "
                                    "         from LawNotices "
                                    "inner join LawNoticeTypes on  LawNotices.LawNoticeTypeId "
                                    "=LawNoticeTypes.LawNoticeTypeId "
                                    "inner join LawNoticeParams on LawNoticeParams.LawNoticeGuid = "
                                    "lawNotices.LawNoticeGuid "
                                    "inner join LawCases on LawCases.LawCaseGuid = LawNotices.LawCaseGuid where "
                                    " LawNoticeStatusId is not null and "
                                    f"LawCases.Account='{P_LSHET}') as nofications_tbl where NoticeParamName is not null",
                                    engin_MS)
    df_nofications = pd.DataFrame(table_nofications)
    df_nofications["RequestDateTime"] = (df_nofications["RequestDateTime"]).apply(
        lambda x: x.date() if not pd.isnull(x) else "")
    if df_nofications.empty:
        return "Такого лицевого счета не существует"
    df_nofications = df_nofications.to_dict('records')
    return df_nofications


def stages_work(P_LSHET):
    # Поэтапная работа в ПС Претензия
    table_stages_work = pd.read_sql(
        "select distinct LawCases.Account, LawCases.AutoDebtWork, LawAlgorithms.Name as AlgorithmName,"
        "LawStages.Name as StageName "
        "from LawNotices "
        "inner join LawCases on LawCases.LawCaseGuid = LawNotices.LawCaseGuid "
        "inner join LawStages on LawCases.LawStageId = LawStages.Id "
        f"inner join LawAlgorithms on LawAlgorithms.OrganizationId = LawStages.OrganizationId where LawCases.Account='{P_LSHET}'",
        engin_MS)
    df_stages_work = pd.DataFrame(table_stages_work)
    if df_stages_work.empty:
        return "Такого лицевого счета не существует"
    df_stages_work = df_stages_work.to_dict('records')
    return df_stages_work


def claim_work(P_LSHET):
    # Исковая работа
    table_claim_work = pd.read_sql("select distinct LawCases.Account, "
                                   "concat('c ', LawSuits.BegDate , ' по ',  LawSuits.EndDate) as PeriodOfStability, "
                                   "LawSuits.CalcEndDate, LawSuitTypes.Name as SuitTypeName, "
                                   "LawSuits.SuitSumma,LawCases.FullName, LawSuits.DutySumma, LawCourts.Name as "
                                   "CourtName, LawSuitStatuses.StatusName "
                                   "from LawNotices "
                                   "inner join LawCases on LawCases.LawCaseGuid = LawNotices.LawCaseGuid "
                                   "inner join LawSuits on LawSuits.LawCaseGuid = LawCases.LawCaseGuid "
                                   "inner join LawSuitTypes on LawSuitTypes.LawSuitTypeId = LawSuits.LawSuitTypeId "
                                   "inner join LawCourts on LawCourts.LawCourtId = LawSuits.LawCourtId "
                                   "inner join LawSuitStatuses on LawSuitStatuses.ExtCode  = "
                                   f"LawSuits.LawSuitStatusId where LawCases.Account='{P_LSHET}'",
                                   engin_MS)
    df_claim_work = pd.DataFrame(table_claim_work)
    if df_claim_work.empty:
        return "Такого лицевого счета не существует"
    df_claim_work = df_claim_work.to_dict('records')
    return df_claim_work
