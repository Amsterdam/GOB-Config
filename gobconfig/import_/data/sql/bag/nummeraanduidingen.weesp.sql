SELECT * FROM (SELECT
    object->>'identificatie' as identificatie,
    object->>'voorkomen/Voorkomen/voorkomenidentificatie' as "voorkomen/Voorkomen/voorkomenidentificatie",
    object->>'huisnummer' as huisnummer,
    object->>'geconstateerd' as geconstateerd,
    object->>'huisletter' as huisletter,
    object->>'huisnummertoevoeging' as huisnummertoevoeging,
    object->>'postcode' as postcode,
    object->>'voorkomen/Voorkomen/beginGeldigheid' as "voorkomen/Voorkomen/beginGeldigheid",
    object->>'voorkomen/Voorkomen/eindGeldigheid' as "voorkomen/Voorkomen/eindGeldigheid",
    object->>'ligtAan/OpenbareRuimteRef' as "ligtAan/OpenbareRuimteRef",
    object->>'typeAdresseerbaarObject' as "typeAdresseerbaarObject",
    object->>'documentdatum' as documentdatum,
    object->>'documentnummer' as documentnummer,
    object->>'status' as status,
    last_update
FROM bag_nummeraanduidingen WHERE gemeente = '0457') bag_nummeraanduidingen
