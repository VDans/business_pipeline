SELECT
    a.object,
    a.reservation_start,
    CASE
        WHEN a.reservation_end >= CURRENT_DATE - 9
        AND a.reservation_start <= CURRENT_DATE - 9
        THEN CURRENT_DATE - 9
        ELSE a.reservation_start
    END AS reservation_start_adjusted,
    a.reservation_end,
    a.guest_name,
    a.platform,
    b.n_guests,
    b.total_amount_paid_by_guest,
    a.booking_id
FROM
    bookings a
LEFT JOIN
    public.bookings_kpis b
ON
    a.booking_id = b.booking_id
AND a.status = b.status
WHERE
    a.status = 'OK'
AND a.reservation_end > CURRENT_DATE - 9
AND a.reservation_start <= CURRENT_DATE + 180

ORDER BY
    a.reservation_start;