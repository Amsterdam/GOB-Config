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
    object->>'voorkomen/Voorkomen/beginGeldigheid' as "voorkomen/Voorkomen/beginGeldigheid",
    object->>'voorkomen/Voorkomen/eindGeldigheid' as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    object->>'voorkomen/Voorkomen/tijdstipRegistratie' as "voorkomen/Voorkomen/tijdstipRegistratie",
    last_update
FROM bag_verblijfsobjecten WHERE gemeente = '0457') bag_verblijfsobjecten
