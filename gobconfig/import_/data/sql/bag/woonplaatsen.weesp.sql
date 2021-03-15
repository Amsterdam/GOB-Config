SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'naam' as naam,
    object->>'status' as status,
    object->>'geconstateerd' as geconstateerd,
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    object->>'geometrie/vlak' as "geometrie/vlak",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/beginGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS') as "voorkomen/Voorkomen/beginGeldigheid",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS')  as "voorkomen/Voorkomen/eindGeldigheid",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS')  as "voorkomen/Voorkomen/tijdstipRegistratie",
    COALESCE(
        to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS'),
        CASE WHEN object->>'status' LIKE '%ingetrokken'
            THEN to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') 
            ELSE NULL
         END) as expirationdate,
    last_update
FROM bag_woonplaatsen WHERE gemeente = '0457') bag_woonplaatsen
