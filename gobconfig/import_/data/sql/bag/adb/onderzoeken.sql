select
    identificatie,
    volgnummer,
    registratiedatum,
    object_identificatie,
    objecttype,
    kenmerk,
    CASE WHEN in_onderzoek THEN 'J' ELSE 'N' END AS in_onderzoek,
    begin_geldigheid,
    eind_geldigheid,
    documentnummer,
    documentdatum,
    tijdstip_registratie,
    eind_registratie,
    _expiration_date
from bag_adb.bag_onderzoeken;