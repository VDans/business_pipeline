SELECT
    a.object,
    a.reservation_start,
    a.adults + a.children AS n_guests,
    COALESCE(b.eta, 'Nicht gesagt') as eta,
    COALESCE(b.beds, 'Nicht gesagt') as beds
FROM
    bookings a
LEFT JOIN
    checkin_data b
ON
    a.booking_id = b.booking_id
WHERE
    a.status = 'OK'
AND a.reservation_start >= CURRENT_DATE - 5
AND a.reservation_start <= CURRENT_DATE + 31
ORDER BY
    a.reservation_start