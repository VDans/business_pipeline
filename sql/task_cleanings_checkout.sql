SELECT
    a.object,
    a.reservation_end,
    CASE
        WHEN LAG(a.reservation_start, -1) OVER
                                                (
                                            PARTITION BY
                                                a.object
                                            ORDER BY
                                                a.reservation_end) = a.reservation_end
        THEN LAG(a.adults + a.children, -1) OVER
                                                  (
                                              PARTITION BY
                                                  a.object
                                              ORDER BY
                                                  a.reservation_end)
        ELSE o.max_capacity
    END AS n_guests,
    CASE
        WHEN LAG(a.reservation_start, -1) OVER
                                                (
                                            PARTITION BY
                                                a.object
                                            ORDER BY
                                                a.reservation_end) = a.reservation_end
        THEN LAG(COALESCE(b.eta, 'Nicht gesagt'), -1) OVER
                                                            (
                                                        PARTITION BY
                                                            a.object
                                                        ORDER BY
                                                            a.reservation_end)
        ELSE 'Noch Nicht Gebucht'
    END                             AS eta,
    COALESCE(b.etd, 'Nicht gesagt') AS etd,
    CASE
        WHEN LAG(a.reservation_start, -1) OVER
                                                (
                                            PARTITION BY
                                                a.object
                                            ORDER BY
                                                a.reservation_end) = a.reservation_end
        THEN LAG(COALESCE(b.beds, 'Nicht gesagt'), -1) OVER
                                                             (
                                                         PARTITION BY
                                                             a.object
                                                         ORDER BY
                                                             a.reservation_end)
        ELSE 'Noch Nicht Gebucht'
    END AS beds
FROM
    bookings a
LEFT JOIN
    checkin_data b
ON
    a.booking_id = b.booking_id
LEFT JOIN
    objects o
ON
    a.object = o.object
WHERE
    a.status = 'OK'
AND a.reservation_end >= CURRENT_DATE - 5
AND a.reservation_end <= CURRENT_DATE + 31
ORDER BY
    a.object;
