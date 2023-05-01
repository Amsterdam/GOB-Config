WITH peilmerken AS (
    SELECT h.nummer                                        AS identificatie
         , x.hoogte                                        AS hoogte_tov_nap
         , EXTRACT(YEAR FROM x.inwindatum)                 AS jaar
         , h.mer_id                                        AS merk_id   -- 0, 1, 2, 7, 10, 14, 15, 16, 17, 20, 99
         , m.omschrijving                                  AS merk_omschrijving
         , REPLACE(h.omschrijving, CHR(10), ' ')           AS omschrijving
         , h.windr                                         AS windrichting
         , h.xmuur                                         AS xmuurvlak
         , h.ymuur                                         AS ymuurvlak
         , h.agi_nummer                                    AS rws_nummer
         , TO_CHAR(sdo_util.to_wktgeometry(h.geom))        AS geometrie
         , h.sta_id                                        AS status_id -- 1=actueel 2=niet te meten 3=vervallen
         , s.omschrijving                                  AS status_omschrijving
         , TO_CHAR(h.vervaldatum, 'YYYY-MM-DD')            AS vervaldatum
         , k.bou_nummer                                    AS ligt_in_bouwblok
         , h.id                                            AS source_id
         , TO_CHAR(h.vervaldatum, 'YYYY-MM-DD HH24:MI:SS') AS expirationdate
         , CASE
               WHEN
                   SUBSTR(h.nummer, 1, 3) = '000'
                   OR h.mer_id IN ('-', '+')
                   OR x.hoogte IS NULL
                   OR h.vervaldatum IS NOT NULL
                   OR h.sta_id NOT IN (1, 2)
               THEN
                   'N'
               ELSE
                   'J'
        END                                                AS publiceerbaar
    FROM grs_hoogtepunten h

    LEFT OUTER JOIN grs_merken m
        ON h.mer_id = m.id
    LEFT OUTER JOIN grs_status s
        ON h.sta_id = s.id
    LEFT OUTER JOIN grs_kringpunten k
        ON h.id = k.hoo_id

    LEFT OUTER JOIN (
        SELECT m1.hoo_id
               , m1.hoogte
               , m1.inwindatum
        FROM grs_metingen_herz m1
        JOIN (
            SELECT m2.hoo_id, MAX(m2.inwindatum) AS inwindatum
            FROM grs_metingen_herz m2
            GROUP BY m2.hoo_id
        ) q
            ON m1.hoo_id = q.hoo_id AND m1.inwindatum = q.inwindatum
    ) x
        ON h.id = x.hoo_id
    -- nap peilmerk = 6
    WHERE h.typ_nummer = 6
)
SELECT *
FROM peilmerken
WHERE identificatie != 99999999 AND publiceerbaar = 'J'
