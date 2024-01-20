SELECT
    a.object,
    CASE
        WHEN a.reservation_end >= CURRENT_DATE - 15
        AND a.reservation_start <= CURRENT_DATE - 15
        THEN CURRENT_DATE - 15
        ELSE a.reservation_start
    END AS reservation_start_adjusted,
    a.reservation_end,
    b.n_guests,
    COALESCE(c.eta, 'Nicht gesagt')  AS eta,
    COALESCE(c.etd, 'Nicht gesagt')  AS etd,
    COALESCE(c.beds, 'Nicht gesagt') AS beds
FROM
    bookings a
LEFT JOIN
    public.bookings_kpis b
ON
    a.booking_id = b.booking_id
AND a.status = b.status
LEFT JOIN
    public.checkin_data c
ON
    a.booking_id = c.booking_id
WHERE
    a.status = 'OK'
AND a.reservation_end > CURRENT_DATE - 14
ORDER BY
    a.reservation_start;