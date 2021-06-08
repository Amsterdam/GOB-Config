select identificatie,
       straatnaam,
       openbare_ruimte,
       huisnummer,
       huisletter,
       huisnummertoevoeging,
       aanduidinghuisnummer,
       postcode,
       adres_compleet,
       ligt_in_woonplaats,
       locatiebeschrijving,
       is_verblijfsobject,
       is_ligplaats,
       is_standplaats,
       heeft_nummeraanduiding,
       verblijft_in_land,
       buitenland_regel_1,
       buitenland_regel_2,
       buitenland_regel_3,

       split_part(datum_aanvang_adres_buitenland, '-', 3) || '-' ||
       split_part(datum_aanvang_adres_buitenland, '-', 2) || '-' ||
       split_part(datum_aanvang_adres_buitenland, '-', 1) as datum_aanvang_adres_buitenland,

       aanduiding_persoongegevens_in_onderzoek,

       split_part(datum_ingang_persoononderzoek, '-', 3) || '-' ||
       split_part(datum_ingang_persoononderzoek, '-', 2) || '-' ||
       split_part(datum_ingang_persoononderzoek, '-', 1) as datum_ingang_persoononderzoek,

       split_part(datum_einde_persoononderzoek, '-', 3) || '-' ||
       split_part(datum_einde_persoononderzoek, '-', 2) || '-' ||
       split_part(datum_einde_persoononderzoek, '-', 1) as datum_einde_persoononderzoek,

       aantal_keren_persoon_in_onderzoek

from brp_prepared.verblijfplaatsen
