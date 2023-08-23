SELECT
    a.object,
    a.reservation_start,
    a.reservation_end,
    a.guest_name,
    a.platform,
    b.n_guests,
    b.total_amount_paid_by_guest,
    a.booking_id
FROM
    bookings a left join public.bookings_kpis b on a.booking_id = b.booking_id
WHERE
    status = 'OK'
    AND reservation_start >= CURRENT_DATE - 14
ORDER BY
    reservation_start;