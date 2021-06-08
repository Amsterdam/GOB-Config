select identificatie,
       van_persoon,
       met_verblijfplaats,
       functieadres,
       gemeente_inschrijving,

       split_part(datum_inschrijving_gemeente, '-', 3) || '-' ||
       split_part(datum_inschrijving_gemeente, '-', 2) || '-' ||
       split_part(datum_inschrijving_gemeente, '-', 1) as datum_inschrijving_gemeente,

       split_part(datum_aanvang_adreshouding, '-', 3) || '-' ||
       split_part(datum_aanvang_adreshouding, '-', 2) || '-' ||
       split_part(datum_aanvang_adreshouding, '-', 1)  as datum_aanvang_adreshouding,

       eind_datum_bewoning,
       reden_einde_bewoning,
       datum_uitschrijving_gemeente,
       gemeente_waarnaar_vertrokken,
       indicatie_actueel_historisch,
       land_vanwaar_ingeschreven,
       datum_vertrek_nederland,
       land_waarnaar_vertrokken,

       split_part(datum_vestiging_nederland, '-', 3) || '-' ||
       split_part(datum_vestiging_nederland, '-', 2) || '-' ||
       split_part(datum_vestiging_nederland, '-', 1)   as datum_vestiging_nederland

from brp_prepared.persoonsverblijfplaatsen
