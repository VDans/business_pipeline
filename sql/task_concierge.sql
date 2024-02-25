SELECT
    object,
    GREATEST(reservation_start, CURRENT_DATE) AS reservation_start,
    reservation_end
FROM
    bookings
WHERE
    status = 'OK'
AND bookings.reservation_end >= CURRENT_DATE
ORDER BY
    reservation_start;