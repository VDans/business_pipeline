SELECT
    bookings.object,
    bookings.reservation_end,
    bookings.adults + bookings.children as n_guests
FROM
    bookings
WHERE
    status = 'OK'
AND reservation_end >= CURRENT_DATE
AND reservation_end <= CURRENT_DATE + 31
ORDER BY
    reservation_end;