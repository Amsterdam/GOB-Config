SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'naam' as naam,
    object->>'status' as status,
    object->>'geconstateerd' as geconstateerd,
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    object->>'geometrie/vlak' as "geometrie/vlak",
    object->>'voorkomen/Voorkomen/beginGeldigheid' as "voorkomen/Voorkomen/beginGeldigheid",
    object->>'voorkomen/Voorkomen/eindGeldigheid' as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'voorkomen/Voorkomen/eindGeldigheid' as "voorkomen/Voorkomen/eindGeldigheid",
    last_update
FROM bag_woonplaatsen WHERE gemeente = '0457') bag_woonplaatsen
