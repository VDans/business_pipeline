SELECT
    *
FROM
    bookings a NATURAL
JOIN
    bookings_kpis b
WHERE
    a.object NOT IN ( 'BERGSTEIG' );