SELECT
    *
FROM
    bookings a
LEFT JOIN
    public.bookings_kpis b
ON
    a.booking_id = b.booking_id;