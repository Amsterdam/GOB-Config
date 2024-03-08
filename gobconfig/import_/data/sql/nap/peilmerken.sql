SELECT
    identificatie,
    hoogte_tov_nap,
    jaar,
    merk::jsonb,
    omschrijving,
    windrichting,
    xmuurvlak,
    ymuurvlak,
    rws_nummer,
    geometrie,
    status::jsonb,
    vervaldatum,
    ligt_in_bouwblok::jsonb AS ligt_in_gebieden_bouwblok,
    source_id,
    expirationdate,
    publiceerbaar
FROM nap_adb.nap_peilmerken;
