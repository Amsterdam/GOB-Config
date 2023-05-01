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
                               REPLACE(h.omschrijving, '(', CHR(32) || CHR(32) || CHR(32)),
                               CHR(32) || CHR(32) || CHR(32),
                               CHR(10)
                           ),
                            CHR(10)
                       ) + 1,
                        LENGTH(h.omschrijving)
                   )
               )
     END                                               AS locatie
     , h.sta_id                                        AS status_id-- 1=actueel 2=niet te meten 3=vervallen
     , s.omschrijving                                  AS status_omschrijving
     , TO_CHAR(h.vervaldatum, 'YYYY-MM-DD')            AS vervaldatum
     , h.mer_id                                        AS merk_id  -- 0, 1, 2, 7, 10, 14, 15, 16, 17, 20, 99
     , m.omschrijving                                  AS merk_omschrijving
     , h.xmuur                                         AS xmuurvlak
     , h.ymuur                                         AS ymuurvlak
     , h.windr                                         AS windrichting
     , k.bou_nummer                                    AS ligt_in_bouwblok
     , x1.hoogte                                       AS hoogte_tov_nap
     , TO_CHAR(x2.inwindatum, 'YYYY-MM-DD')            AS datum
     , CASE
           WHEN
               h.typ_nummer = 6
           THEN
               h.nummer
     END                                               AS is_peilmerk
     , TO_CHAR(sdo_util.to_wktgeometry(h.geom))        AS geometrie
     , CASE
           WHEN
               h.vervaldatum IS NOT NULL
           THEN
               'N'
           ELSE
               'J'
     END                                               AS publiceerbaar
     , h.id                                            AS source_id
     , TO_CHAR(h.vervaldatum, 'YYYY-MM-DD HH24:MI:SS') AS expirationdate
FROM grs_hoogtepunten h

LEFT OUTER JOIN grondslag.grs_kringpunten k
    ON h.id = k.hoo_id
LEFT OUTER JOIN grondslag.grs_status s
    ON h.sta_id = s.id
LEFT OUTER JOIN grondslag.grs_merken m
    ON h.mer_id = m.id

LEFT OUTER JOIN (
    SELECT m1.hoo_id
           , m1.hoogte
           , m1.inwindatum
    FROM grondslag.grs_metingen_herz m1
    JOIN (
        SELECT m2.hoo_id, MAX(m2.inwindatum) AS inwindatum
        FROM grondslag.grs_metingen_herz m2
        GROUP BY m2.hoo_id
    ) q1
        ON m1.hoo_id = q1.hoo_id AND m1.inwindatum = q1.inwindatum
) x1
    ON h.id = x1.hoo_id

LEFT OUTER JOIN (
    SELECT m3.hoo_id
            , m3.hoogte
            , m3.inwindatum
    FROM grondslag.grs_metingen_herz m3
    JOIN (
        SELECT m4.hoo_id, MAX(m4.inwindatum) AS inwindatum
        FROM grondslag.grs_metingen_herz m4
        GROUP BY m4.hoo_id
    ) q2
        ON m3.hoo_id = q2.hoo_id AND m3.inwindatum = q2.inwindatum
) x2
    ON h.id = x2.hoo_id

WHERE (
    -- nap peilmerk
    h.nummer IN (
        SELECT h1.nummer
        FROM grondslag.grs_hoogtepunten h1
        JOIN grondslag.grs_met_ref_punten_herz r
            ON h1.id = r.hoo_id
        WHERE h1.typ_nummer = 6
    )
    OR
    -- ondergrondsmerk
    h.nummer IN (
        SELECT h1.nummer
        FROM grondslag.grs_hoogtepunten h1
        JOIN grondslag.grs_met_ref_punten_herz r
            ON h1.id = r.hoo_id
        WHERE h1.typ_nummer = 9
    )
    OR
    -- referentiebout
    h.typ_nummer = 8
)
AND h.vervaldatum IS NULL -- alleen publiceerbaar
