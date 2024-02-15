select
    identificatie,
    volgnummer,
    registratiedatum,
    begin_geldigheid,
    eind_geldigheid,
    geconstateerd,
    status::jsonb,
    heeft_hoofdadres::jsonb,
    heeft_nevenadres::jsonb,
    geometrie,
    heeft_onderzoeken::jsonb,
    documentdatum,
    documentnummer,
    ligt_in_buurt::jsonb,
    heeft_dossier::jsonb,
    bagproces::jsonb,
    gebruiksdoel::jsonb,
    ligt_in_gemeente::jsonb,
    _expiration_date
from bag_adb.bag_standplaatsen;