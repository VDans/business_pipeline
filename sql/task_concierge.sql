SELECT
    object,
    reservation_start,
    reservation_end
FROM
    bookings
WHERE
    status = 'OK'
AND reservation_start > CURRENT_DATE
ORDER BY
    reservation_start;