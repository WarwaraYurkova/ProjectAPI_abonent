from fastapi import FastAPI

from get_CS_data import general_info_abonent_telephones, email, accrual_and_payment_history, \
    citizens_and_benefits, additional_house_ch, \
    house_characteristics, consumption
from get_Claim_data import notifications, stages_work, claim_work

app = FastAPI()
# Чтение данных из РС
@app.get("/ABONENTS-INFO_ABONENT_TELEPHONES/")
async def get_ABONENTS(P_LSHET: str):
    INFO = general_info_abonent_telephones(P_LSHET)
    #if len(INFO)==0:
     #return "Такого лицевого счета нет в БД"
    #return INFO
    print(INFO)

@app.get("/EMAIL/")
async def get_email(P_LSHET: str):
    INFO=email(P_LSHET)
    if len(INFO)==0:
        return "По данному лицевому счету запись об электронной почте отсутствует"
    return INFO


@app.get("/ACCURAL_AND_PAYMENT_HYSTORY/")
async def get_accrl_pmnt_history(P_LSHET: str):
    INFO=accrual_and_payment_history(P_LSHET)
    if len(INFO)==0:
        return "По данному лицес=вому счету запись об истории начислений и оплате отсутствует"
    return INFO

@app.get("/ABONENTS_CITIZENS_BENEFITS/")
async def get_CITIZENS(P_LSHET: str):
    INFO = citizens_and_benefits(P_LSHET)
    if len(INFO) == 0:
        return "Такого лицевого счета нет в БД"
    return INFO
@app.get("/СONSUMPTION/")
async def get_consumption(P_LSHET: str):
    INFO = consumption(P_LSHET)
    if len(INFO) == 0:
        return "Такого лицевого счета нет в БД"
    return INFO
#@app.get("/ABONENTS-LAWSUITS_CLAIMS/")
#async def get_LAWSUITS(P_LSHET: str):
    #INFO = lawsuits_claims(P_LSHET)
    #return INFO

@app.get("/HOUSE_CHARACTERISTICS/")
async def get_HOUSE_CHARACTERISTICS(P_LSHET: str):
    INFO = house_characteristics(P_LSHET)
    return INFO

@app.get("/HOUSE_ADDITIONAL_CHARACTERISTICS/")
async def get_HOUSE_ADDITIONAL_CHARACTERISTICS(P_LSHET: str):
    INFO = additional_house_ch(P_LSHET)
    if len(INFO) == 0:
        return "Такого лицевого счета нет в БД"
    return INFO

# Чтение данных из Претензии
@app.get("/CLAIM-NOTIFICATIONS/")
async def get_CLAIM_NOFICATIONS(P_LSHET: str):
    INFO = notifications(P_LSHET)

    return INFO

@app.get("/STAGES_WORK/")
async def get_stages_work(P_LSHET: str):
    INFO = stages_work(P_LSHET)

    return INFO

@app.get("/CLAIM_WORK/")
async def get_claim_work(P_LSHET: str):
    INFO = claim_work(P_LSHET)

    return INFO

# Запись
