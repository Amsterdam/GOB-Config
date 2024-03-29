select
    identificatie,
    volgnummer,
    registratiedatum,
    begin_geldigheid,
    eind_geldigheid,
    geconstateerd,
    status::jsonb,
    heeft_onderzoeken::jsonb,
    documentdatum,
    documentnummer,
    type::jsonb,
    naam,
    naam_nen,
    ligt_in_woonplaats::jsonb,
    beschrijving_naam,
    heeft_dossier::jsonb,
    bagproces::jsonb,
    geometrie,
    _expiration_date
from bag_adb.bag_openbareruimtes;