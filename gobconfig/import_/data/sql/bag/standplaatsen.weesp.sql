SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'geconstateerd' as geconstateerd,
    object->>'voorkomen/Voorkomen/beginGeldigheid' as "voorkomen/Voorkomen/beginGeldigheid",
    object->>'voorkomen/Voorkomen/eindGeldigheid' as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'status' as status,
    object->>'heeftAlsHoofdadres/NummeraanduidingRef' as "heeftAlsHoofdadres/NummeraanduidingRef",
    object->>'heeftAlsNevenadres/NummeraanduidingRef' as "heeftAlsNevenadres/NummeraanduidingRef",
    object->>'geometrie' as geometrie,
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    last_update
FROM bag_standplaatsen WHERE gemeente = '0457') bag_standplaatsen

