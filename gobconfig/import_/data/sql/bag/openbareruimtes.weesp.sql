SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'status' as status,
    object->>'voorkomen/Voorkomen/beginGeldigheid' as "voorkomen/Voorkomen/beginGeldigheid",
    object->>'voorkomen/Voorkomen/eindGeldigheid' as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'geconstateerd' as geconstateerd,
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    object->>'naam' as naam,
    object->>'ligtIn/WoonplaatsRef' as "ligtIn/WoonplaatsRef",
    object->>'type' as type,
    last_update
FROM bag_openbareruimtes WHERE gemeente = '0457') bag_openbareruimtes
