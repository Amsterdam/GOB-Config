SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'geconstateerd' as geconstateerd,
    object->>'heeftAlsHoofdadres/NummeraanduidingRef' as "heeftAlsHoofdadres/NummeraanduidingRef",
    object->>'heeftAlsNevenadres/NummeraanduidingRef' as "heeftAlsNevenadres/NummeraanduidingRef",
    object->>'geometrie/punt' as "geometrie/punt",
    object->>'gebruiksdoel' as gebruiksdoel,
    object->>'oppervlakte' as oppervlakte,
    object->>'status' as status,
    object->>'maaktDeelUitVan/PandRef' as "maaktDeelUitVan/PandRef",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/beginGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS')  as "voorkomen/Voorkomen/beginGeldigheid",
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS')  as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') as "voorkomen/Voorkomen/tijdstipRegistratie",
    COALESCE(
        to_char(to_timestamp(object->>'voorkomen/Voorkomen/eindGeldigheid', 'YYYY-MM-DD'), 'YYYY-MM-DD HH24:MI:SS') ,
        CASE WHEN object->>'status' ~* 'Niet gerealiseerd verblijfsobject|Verblijfsobject ingetrokken|Verblijfsobject ten onrechte opgevoerd'
            THEN to_char(to_timestamp(object->>'voorkomen/Voorkomen/tijdstipRegistratie', 'YYYY-MM-DD HH24:MI:SS'), 'YYYY-MM-DD HH24:MI:SS') 
            ELSE NULL
         END) as expirationdate,
    last_update
FROM bag_verblijfsobjecten WHERE gemeente = '0457') bag_verblijfsobjecten
