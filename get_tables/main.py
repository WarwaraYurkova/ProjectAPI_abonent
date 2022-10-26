from fastapi import FastAPI, Request
import uvicorn
from get_CS_data import general_info_abonent_telephones, email, accrual_and_payment_history, \
    citizens_and_benefits, additional_house_ch, \
    house_characteristics, consumption,consumption_parameters, lawsuits_claims, equipment
from get_Claim_data import notifications, stages_work, claim_work
from post_CS_data import post_general_info_abonent, post_telephones_abonent, post_email


if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, host="127.0.0.1", reload=True)

app = FastAPI()
# Чтение данных из РС
@app.get("/ABONENTS-INFO_ABONENT_TELEPHONES/")
async def get_ABONENTS(P_LSHET: str):
    INFO = general_info_abonent_telephones(P_LSHET)
    if len(INFO)==0:
     return "Такого лицевого счета нет в БД"
    return INFO
    #print(INFO)

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
        return "По данному лицевому счету запись отсутствует"
    return INFO

@app.get("/СONSUMPTION/")
async def get_consumption(P_LSHET: str):
    INFO = consumption(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO
@app.get("/EQUIPMENT/")
async def get_equipment(P_LSHET: str):
    INFO = equipment(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO
@app.get("/ABONENTS-LAWSUITS_CLAIMS/")
async def get_LAWSUITS(P_LSHET: str):
    INFO = lawsuits_claims(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO

@app.get("/HOUSE_CHARACTERISTICS/")
async def get_HOUSE_CHARACTERISTICS(P_LSHET: str):
    INFO = house_characteristics(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO

@app.get("/HOUSE_ADDITIONAL_CHARACTERISTICS/")
async def get_HOUSE_ADDITIONAL_CHARACTERISTICS(P_LSHET: str):
    INFO = additional_house_ch(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO

@app.get("/CONSUMPTION_PARAMETERS/")
async def GET_CONSUMPTION_PARAMETERS(P_LSHET:str):
    INFO=consumption_parameters(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO

# Чтение данных из Претензии
@app.get("/CLAIM-NOTIFICATIONS/")
async def get_CLAIM_NOFICATIONS(P_LSHET: str):
    INFO = notifications(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO

@app.get("/STAGES_WORK/")
async def get_stages_work(P_LSHET: str):
    INFO = stages_work(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO

@app.get("/CLAIM_WORK/")
async def get_claim_work(P_LSHET: str):
    INFO = claim_work(P_LSHET)
    if len(INFO) == 0:
        return "По данному лицевому счету запись отсутствует"
    return INFO


# Запись
@app.post("/GENERAL_INFO_LS/")
async def post_general_info(DOCCD: int,
                            USERCD: int,
                            CHANGE_DATA: str,
                            AGREEMENTPERSONALINFO: int,
                            LSHET: str,
                            CONSENT_TO_MAIL: int):
    post_general_info_abonent(DOCCD,USERCD,CHANGE_DATA,AGREEMENTPERSONALINFO,LSHET,CONSENT_TO_MAIL)

@app.post("/PHONES_LS/")
async def post_telephones( PHONETYPE: int,
                                OWNERTYPEID: int,
                                LSHET: str,
                                PHONENUMBER: str,
                                SOURCEID: int,
                                USERCD: int,
                                CHANGE_DATA:str):
    post_telephones_abonent(LSHET, PHONETYPE, PHONENUMBER, SOURCEID, OWNERTYPEID,USERCD,CHANGE_DATA)
    return {
        "JSON Load ": {"phonetype": PHONETYPE,"phonenumber": PHONENUMBER, "OWNERTYPEID": OWNERTYPEID, "LSHET": LSHET,
               "COMMDATE": CHANGE_DATA,"SOURCEID": SOURCEID, "USERCD":USERCD,"CHANGE_DATA":CHANGE_DATA,"TS":CHANGE_DATA},
    }
@app.post("/EMAIL_LS/")
async def post_emails(LSHET: str,
                      USERCD: int,
                      EMAIL: str,
                      EMAILTYPEID:int,
                      OWNERTYPEID: int,
                      SOURCEID: int,
                      CHANGE_DATA: str):
    post_email(LSHET, EMAILTYPEID, EMAIL, CHANGE_DATA, OWNERTYPEID, SOURCEID, USERCD)
    return {
        "JSON Load ": {"LSHET": LSHET,"USERCD": USERCD, "EMAIL": EMAIL, "EMAILTYPEID": EMAILTYPEID,
               "OWNERTYPEID": OWNERTYPEID,"SOURCEID": SOURCEID, "CHANGE_DATA":CHANGE_DATA,"TS":CHANGE_DATA},
    }

