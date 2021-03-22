SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'status' as status,
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/beginGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS') as "voorkomen/Voorkomen/beginGeldigheid",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS')  as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'geconstateerd' as geconstateerd,
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    object->>'naam' as naam,
    object->>'ligtIn/WoonplaatsRef' as "ligtIn/WoonplaatsRef",
    object->>'type' as type,
    object->>'verkorteNaam/VerkorteNaamOpenbareRuimte/verkorteNaam' as "verkorteNaam/VerkorteNaamOpenbareRuimte/verkorteNaam",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') as "voorkomen/Voorkomen/tijdstipRegistratie",
    COALESCE(
        to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS'), 
        CASE WHEN object->>'status' LIKE '%ingetrokken'
            THEN 
                to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') 
            ELSE NULL
         END) as expirationdate,
    last_update
FROM bag_openbareruimtes WHERE gemeente = '0457') bag_openbareruimtes
