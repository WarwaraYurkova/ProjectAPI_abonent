import pandas as pd
import sqlalchemy

from CSconnect import engine
import numpy as np


def normalize(df, fields_list, default, newtype):
    df[fields_list] = df[fields_list].fillna(default).astype(newtype)
    return df


def address(P_LSHET):
    # Общая информация об абоненте, Контактные телефоны, Электронная почта
    abonents = sqlalchemy.text(f"select * from abonents where lshet={P_LSHET}")
    houses = sqlalchemy.text(
        f"select * from houses where housecd in (select housecd from abonents where lshet={P_LSHET})")
    street = sqlalchemy.text(
        f"select * from street where streetcd in (select streetcd from houses where housecd in (select housecd from abonents where lshet={P_LSHET}))")
    punkt = sqlalchemy.text("select * from punkt")
    district = sqlalchemy.text("select * from district")
    regiondistricts = sqlalchemy.text("select * from regiondistricts")
    settlements = sqlalchemy.text("select * from settlements")

    abonents = (engine.execute(abonents)).all()
    street = (engine.execute(street)).all()
    houses = (engine.execute(houses)).all()
    punkt = (engine.execute(punkt)).all()
    district = (engine.execute(district)).all()
    regiondistricts = (engine.execute(regiondistricts)).all()
    settlements = (engine.execute(settlements)).all()

    abonents = pd.DataFrame(abonents)
    street = pd.DataFrame(street)
    houses = pd.DataFrame(houses)
    punkt = pd.DataFrame(punkt)
    district = pd.DataFrame(district)
    regiondistricts = pd.DataFrame(regiondistricts)
    settlements = pd.DataFrame(settlements)

    a = abonents.drop(index=abonents[abonents['housecd'] < 0].index)
    houses = houses.set_index('housecd')
    street = street.set_index(['punktcd', 'streetcd'])
    punkt = punkt.set_index('punktcd')
    district = district.set_index('distcd')
    regiondistricts = regiondistricts.set_index('regiondistrictcd')
    a['housecd'] = a['housecd'].astype(np.int32)
    a = a.join(houses[['streetcd', 'punktcd', 'houseno', 'housepostfix', 'korpusno', 'korpuspostfix', 'distcd']],
               on='housecd')
    a = a.join(street[['streetnm']], on=['punktcd', 'streetcd'])
    a = a.join(punkt[['regiondistrictcd', 'settlementid', 'punktnm']], on='punktcd')
    normalize(a, ['streetcd', 'regiondistrictcd', 'settlementid'], -1, np.int32)
    a = pd.concat([a,district])
    a = a.join(regiondistricts[['regiondistrictnm']], on='regiondistrictcd', how="inner")
    a.loc[a["regiondistrictnm"].isnull(), "regiondistrictnm"] = "Не указан"
    if len(settlements) > 0:
        settlements = settlements.set_index('settlementid')
        a = a.join(settlements[['settlementname']], on='settlementid', how="inner")
        a.loc[a["settlementname"].isnull(), "settlementname"] = a["punktnm"]
    else:
        a["settlementid"] = -1
        a["settlementname"] = a["punktnm"]
    a["houseno"] = a["houseno"].astype(int)
    # Собираем строку адреса
    a['address'] = a['punktnm'] + ", " + a['streetnm'] + ', '
    a['dom_add'] = ''

    # Номер дома

    mask_houseno = a['houseno'] > 0
    mask_housepostfix = a['housepostfix'].notnull()
    a.loc[mask_houseno | mask_housepostfix, 'dom_add'] = a.loc[mask_houseno | mask_housepostfix, 'dom_add'] + 'д.'
    a.loc[mask_houseno, 'dom_add'] = a.loc[mask_houseno, 'dom_add'] + a.loc[mask_houseno, 'houseno'].astype(str)
    a.loc[mask_housepostfix, 'dom_add'] = a.loc[mask_housepostfix, 'dom_add'] + a.loc[
        mask_housepostfix, 'housepostfix']

    # Корпус
    mask_korpusno = a['korpusno'] > 0
    mask_korpuspostfix = a['korpuspostfix'].notnull()
    a.loc[mask_korpusno | mask_korpuspostfix, 'dom_add'] = a.loc[
                                                               mask_korpusno | mask_korpuspostfix, 'dom_add'] + ', к.'
    a.loc[mask_korpusno, 'dom_add'] = a.loc[mask_korpusno, 'dom_add'] + a.loc[mask_korpusno, 'korpusno'].astype(
        'str')
    a.loc[mask_korpuspostfix, 'dom_add'] = a.loc[mask_korpuspostfix, 'dom_add'] + a.loc[
        mask_korpuspostfix, 'korpuspostfix']
    a.loc[a['dom_add'].notnull(), 'address'] = a.loc[a['dom_add'].notnull(), 'address'] + a.loc[
        a['dom_add'].notnull(), 'dom_add']

    # Квартира
    mask_flatno = a['flatno'] > 0
    mask_flatpostfix = a['flatpostfix'].notnull()
    a.loc[mask_flatno | mask_flatpostfix, 'address'] = a.loc[mask_flatno | mask_flatpostfix, 'address'] + ', кв.'
    a.loc[mask_flatno, 'address'] = a.loc[mask_flatno, 'address'] + a.loc[mask_flatno, 'flatno'].astype('str')
    a.loc[mask_flatpostfix, 'address'] = a.loc[mask_flatpostfix, 'address'] + a.loc[mask_flatpostfix, 'flatpostfix']

    # Комната
    mask_roomno = a['roomno'] > 0
    mask_roompostfix = a['roompostfix'].notnull()
    a.loc[mask_roomno | mask_roompostfix, 'address'] = a.loc[mask_roomno | mask_roompostfix, 'address'] + ', ком.'
    a.loc[mask_roomno, 'address'] = a.loc[mask_roomno, 'address'] + a.loc[mask_roomno, 'roomno'].astype('str')
    a.loc[mask_roompostfix, 'address'] = a.loc[mask_roompostfix, 'address'] + a.loc[mask_roompostfix, 'roompostfix']
    return a
