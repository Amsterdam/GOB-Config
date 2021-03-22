SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'geconstateerd' as geconstateerd,
    object->>'geometrie'::text as geometrie,
    object->>'oorspronkelijkBouwjaar' as "oorspronkelijkBouwjaar",
    object->>'status' as status,
    object->>'voorkomen/Voorkomen/beginGeldigheid' as "voorkomen/Voorkomen/beginGeldigheid",
    object->> 'voorkomen/Voorkomen/eindGeldigheid' as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    last_update
FROM bag_panden WHERE gemeente = '0457') bag_panden
