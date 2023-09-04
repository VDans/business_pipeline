SELECT
    COALESCE(c.gv_country, '100')                                               AS "Codex",
    ''                                                                         AS "Land",
    COUNT(*)                                                                    AS "Ankünfte",
    SUM(a.reservation_end - a.reservation_start) * SUM((a.adults + a.children)) AS "Übernachtungen"
FROM
    bookings a
LEFT JOIN
    checkin_data b
ON
    a.booking_id = b.booking_id
LEFT JOIN
    dict_gv c
ON
    b.nationality = c.local_country
WHERE
    a.status = 'OK'
AND a.object in ({0})
AND a.reservation_start BETWEEN {1} AND {2}
GROUP BY
    1
ORDER BY
    1;