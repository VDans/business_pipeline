SELECT
    a.object,
    a.reservation_start,
    a.guest_name,
    b.n_guests,
    b.total_amount_paid_by_guest
FROM
    bookings a left join public.bookings_kpis b on a.booking_id = b.booking_id
WHERE
    status = 'OK'
ORDER BY
    reservation_start;