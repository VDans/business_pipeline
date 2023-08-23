SELECT
    a.object,
    a.reservation_start,
    a.platform,
    a.guest_name,
    a.phone,
    a.email,
    a.booking_id
FROM
    bookings a
LEFT JOIN
    checkin_data b
ON
    a.booking_id = b.booking_id
WHERE
    a.status = 'OK'
AND a.object IN
    (   SELECT
            flat_name
        FROM
            messages)
AND a.reservation_start >= CURRENT_DATE
AND a.reservation_start < CURRENT_DATE + 3
AND b.complete_name IS NULL
ORDER BY
    2;