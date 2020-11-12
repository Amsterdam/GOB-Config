SELECT *
FROM (
         SELECT dossier                                             AS dossier,
                max(item_created)                                   AS registratiedatum,
                string_agg(cast(subject2 as nvarchar(max)), ';')
                within group (order by subject2)                    AS heeft_bag_brondocument
         FROM (
                  SELECT substring(b.subject2, 0, patindex('%[_ ]%', b.subject2)) AS dossier
                       , b.item_created
                       , b.subject2
                  FROM dbo.item b
                           JOIN dbo.item d ON d.item_key = b.it_parent_key
                  WHERE d.item_deleted IS NULL
                    AND b.item_deleted IS NULL
                    AND b.subject2 IS NOT NULL
                    AND UPPER(substring(b.subject1, 8, 2)) IN
                        ('GV', 'MB', 'PC', 'RO', 'SA', 'SB', 'SC', 'SD', 'SE', 'SF', 'SG', 'SH', 'SJ', 'SK', 'SM',
                         'SN',
                         'SP', 'SQ', 'SR', 'ST', 'SU', 'SV', 'SW', 'TM')
              ) q
         GROUP BY dossier
     ) r