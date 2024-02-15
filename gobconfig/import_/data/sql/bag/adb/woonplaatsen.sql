select
    identificatie,
    volgnummer,
    registratiedatum,
    begin_geldigheid,
    eind_geldigheid,
    naam,
    geometrie,
    geconstateerd,
    heeft_onderzoeken::jsonb,
    documentdatum,
    documentnummer,
    status::jsonb,
    ligt_in_gemeente::jsonb,
    heeft_dossier::jsonb,
    bagproces,
    _expiration_date
from bag_adb.bag_woonplaatsen;