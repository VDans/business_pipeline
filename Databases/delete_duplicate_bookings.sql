DELETE FROM bookings
WHERE booking_number IN
    (SELECT booking_number
    FROM
        (SELECT booking_number,
         ROW_NUMBER() OVER( PARTITION BY booking_number
        ORDER BY  booking_number ) AS row_num
        FROM bookings ) t
        WHERE t.row_num > 1 )