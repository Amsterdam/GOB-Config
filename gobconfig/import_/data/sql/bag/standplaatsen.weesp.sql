SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'geconstateerd' as geconstateerd,
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/beginGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS')  as "voorkomen/Voorkomen/beginGeldigheid",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS')  as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'status' as status,
    object->>'heeftAlsHoofdadres/NummeraanduidingRef' as "heeftAlsHoofdadres/NummeraanduidingRef",
    object->>'heeftAlsNevenadres/NummeraanduidingRef' as "heeftAlsNevenadres/NummeraanduidingRef",
    object->>'geometrie' as geometrie,
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') as "voorkomen/Voorkomen/tijdstipRegistratie",
    COALESCE(
        to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS'),
        CASE WHEN object->>'status' LIKE '%ingetrokken'
            THEN to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') 
            ELSE NULL
         END) as expirationdate,
    last_update
FROM bag_standplaatsen WHERE gemeente = '0457') bag_standplaatsen

