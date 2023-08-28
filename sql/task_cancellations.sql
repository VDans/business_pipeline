SELECT
    booking_id,
    object,
    platform,
    reservation_start,
    nights_price,
    cleaning
FROM
    bookings
WHERE
    status = 'Cancelled'
ORDER BY
    reservation_start;