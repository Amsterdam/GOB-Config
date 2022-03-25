SELECT *
FROM (
         SELECT dossier                                             AS dossier,
                MAX(item_created)                                   AS registratiedatum,
                STRING_AGG(CAST(subject2 AS NVARCHAR(MAX)), ';')
                WITHIN GROUP (ORDER BY subject2)                    AS heeft_bag_brondocument
         FROM (
                  SELECT SUBSTRING(b.subject2, 0, PATINDEX('%[_ ]%', b.subject2)) AS dossier
                       , b.item_created
                       , b.subject2
                  FROM dbo.item b
                           JOIN dbo.item d ON d.item_key = b.it_parent_key
                  WHERE d.item_deleted IS NULL
                    AND b.item_deleted IS NULL
                    AND b.subject2 IS NOT NULL
                    AND UPPER(SUBSTRING(b.subject1, 8, 2)) IN
                        (
                            'GV', -- Gegevensbeheer (Basisinformatie)
                            'MB', -- Omgevingsdienst NZKG
                            'PC', -- PostNL
                            'RO', -- Ruimte en Duurzaamheid
                            'SB', -- Straatnaambesluit
                            'TM', -- Terugmelding

                            -- 'BD' Bestuursdienst
                            -- 'DW' Wonen
                            -- 'OG' Grond en Ontwikkeling
                            -- 'ZA' Zuidas

                            'SA', -- Stadsdeel Centrum
                            'SC', -- Stadsdeel Westerpark
                            'SD', -- Stadsdeel Oud-West
                            'SE', -- Stadsdeel West
                            'SF', -- Stadsdeel Nieuw-West
                            'SG', -- Stadsdeel Zeeburg
                            'SH', -- Stadsdeel Bos en Lommer
                            'SJ', -- Stadsdeel De Baarsjes
                            'SK', -- Stadsdeel Zuid
                            'SM', -- Stadsdeel Oost
                            'SN', -- Stadsdeel Noord
                            'SP', -- Stadsdeel Geuzenveld-Slotermeer
                            'SQ', -- Stadsdeel Osdorp
                            'SR', -- Stadsdeel Slotervaart
                            'ST', -- Stadsdeel Zuidoost
                            'SU', -- Stadsdeel Oost-Watergraafsmeer
                            'SV', -- Stadsdeel Oud-Zuid
                            'SW', -- Stadsdeel Zuideramstel

                            'WP'  -- Stadsgebied Weesp
                        )
              ) q
         GROUP BY dossier
     ) r
