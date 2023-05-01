-- CHR(32) = ' ', CHR(10) = '\n'
SELECT h.nummer                                        AS identificatie
     , CASE
           WHEN
               INSTR(
                    REPLACE(
                        REPLACE(h.omschrijving, CHR(32) || CHR(32) || CHR(32), CHR(10)),
                        '(', CHR(10)
                    ),
                   CHR(10)
               ) = 0
           THEN
                SUBSTR(h.omschrijving, 1, LENGTH(h.omschrijving))
           ELSE
               SUBSTR(h.omschrijving, 1,
                   INSTR(
                       REPLACE(
                           REPLACE(h.omschrijving, CHR(32) || CHR(32) || CHR(32), CHR(10)),
                           '(', CHR(10)
                       ),
                       CHR(10)
                   ) - 1
               )
    END                                                AS adres
     , CASE
           WHEN
               INSTR(
                    REPLACE(
                        REPLACE(h.omschrijving, CHR(32) || CHR(32) || CHR(32), CHR(10)),
                        '(', CHR(10)
                    ),
                   CHR(10)
               ) = 0
           THEN
               NULL
           ELSE
               LTRIM(
                   SUBSTR(h.omschrijving,
                       INSTR(
                           REPLACE(
                               REPLACE(h.omschrijving, '(', CHR(32) || CHR(32) || CHR(32)), CHR(32) || CHR(32) || CHR(32), CHR(10)
                           ),
                           CHR(10)
                       ) + 1, LENGTH(h.omschrijving)
                   )
               )
    END                                                AS locatie
     , h.sta_id                                        AS status_id     -- 1=actueel 2=niet te meten 3=vervallen
     , s.omschrijving                                  AS status_omschrijving
     , TO_CHAR(h.vervaldatum, 'YYYY-MM-DD')            AS vervaldatum
     , h.mer_id                                        AS merk_id       -- 0, 1, 2, 7, 10, 14, 15, 16, 17, 20, 99
     , m.omschr_verkort                                AS merk_omschrijving
     , h.xmuur                                         AS xmuurvlak
     , h.ymuur                                         AS ymuurvlak
     , h.windr                                         AS windrichting
     , k.bou_nummer                                    AS ligt_in_bouwblok
     , TO_CHAR(sdo_util.to_wktgeometry(h.geom))        AS geometrie
     , h.orde                                          AS publiceerbaar -- publiceren ja (1) of nee (0), fout (leeg)
     , h.id                                            AS source_id
     , TO_CHAR(h.vervaldatum, 'YYYY-MM-DD HH24:MI:SS') AS expirationdate
FROM grondslag.grs_hoogtepunten h
LEFT OUTER JOIN grondslag.grs_kringpunten k
     ON h.id = k.hoo_id
LEFT OUTER JOIN grondslag.grs_merken m
     ON h.mer_id = m.id
LEFT OUTER JOIN grondslag.grs_status s
     ON h.sta_id = s.id
-- deformatiebout (7) / referentiebout(8)
-- alleen publiceerbaar
WHERE (h.typ_nummer = 7 OR h.typ_nummer = 8) AND h.orde = 1
