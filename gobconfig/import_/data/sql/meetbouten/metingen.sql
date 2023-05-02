WITH metingen AS (
    SELECT m.id                                             AS identificatie
         , h1.nummer                                        AS hoort_bij_meetbout
         , TO_CHAR(m.inwindatum, 'YYYY-MM-DD')              AS datum
         , m.wijze_inwinning                                AS wijze_van_inwinnen_id
         , i.omschrijving                                   AS wijze_van_inwinnen
         , m.hoogte                                         AS hoogte_tov_nap
         , q.nummer                                         AS refereert_aan_refpunt
         , b.omschrijving                                   AS is_gemeten_door_onderneming
         , h1.orde                                          AS publiceerbaar -- publiceren ja (1) of nee (0)
         , m.id                                             AS source_id
         , TO_CHAR(h1.vervaldatum, 'YYYY-MM-DD HH24:MI:SS') AS expirationdate
    FROM grondslag.grs_hoogtepunten h1
    JOIN grondslag.grs_metingen_herz m
        ON h1.id = m.hoo_id

    LEFT OUTER JOIN (
        SELECT r.met_id, listagg(h2.nummer, ';') WITHIN GROUP (ORDER BY r.met_id) AS nummer
        FROM grondslag.grs_met_ref_punten_herz r
        JOIN grondslag.grs_hoogtepunten h2 ON r.hoo_id = h2.id
        WHERE h2.typ_nummer != 7
        GROUP BY r.met_id
    ) q ON m.id = q.met_id

    LEFT OUTER JOIN grondslag.grs_wijzen_inwinning i
        ON m.wijze_inwinning = i.id
    LEFT OUTER JOIN grondslag.grs_bronnen b
        ON m.bro_id = b.id
    -- deformatiebout (7) / referentiebout(8) + publiceerbaar
    WHERE (h1.typ_nummer = 7 OR h1.typ_nummer = 8) AND h1.orde = 1
    ORDER BY datum
)
SELECT *
FROM metingen
