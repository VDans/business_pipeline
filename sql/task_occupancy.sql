SELECT
    booking_id,
    object,
    reservation_start,
    reservation_end - 1 as "reservation_end"
FROM
    bookings
WHERE
    status = 'OK'
ORDER BY
    object,
    reservation_start