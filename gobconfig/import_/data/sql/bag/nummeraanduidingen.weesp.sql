SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'huisnummer' as huisnummer,
    object->>'geconstateerd' as geconstateerd,
    object->>'huisletter' as huisletter,
    object->>'huisnummertoevoeging' as huisnummertoevoeging,
    object->>'postcode' as postcode,
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/beginGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS') as "voorkomen/Voorkomen/beginGeldigheid",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS') as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'ligtAan/OpenbareRuimteRef' as "ligtAan/OpenbareRuimteRef",
    object->>'typeAdresseerbaarObject' as "typeAdresseerbaarObject",
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    object->>'status' as status,
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') as "voorkomen/Voorkomen/tijdstipRegistratie",
    CASE WHEN adres_id IS NULL
        THEN 'Nevenadres'
        ELSE 'Hoofdadres'
    END as type_adres,
    COALESCE(
        to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS'),
        CASE WHEN object->>'status' LIKE '%ingetrokken'
            THEN to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') 
            ELSE NULL
         END) as expirationdate,
    last_update
FROM bag_nummeraanduidingen bn
LEFT OUTER JOIN(
        SELECT MAX(object->>'heeftAlsHoofdadres/NummeraanduidingRef') AS adres_id FROM bag_verblijfsobjecten WHERE gemeente = '0457' GROUP BY object->>'heeftAlsHoofdadres/NummeraanduidingRef'
        UNION
        SELECT MAX(object->>'heeftAlsHoofdadres/NummeraanduidingRef') AS adres_id FROM bag_ligplaatsen WHERE gemeente = '0457' GROUP BY object->>'heeftAlsHoofdadres/NummeraanduidingRef'
        UNION
        SELECT MAX(object->>'heeftAlsHoofdadres/NummeraanduidingRef') AS adres_id FROM bag_standplaatsen WHERE gemeente = '0457' GROUP BY object->>'heeftAlsHoofdadres/NummeraanduidingRef'
    ) as q2 ON bn.object->>'identificatie' = adres_id
WHERE gemeente = '0457'
) bag_nummeraanduidingen
