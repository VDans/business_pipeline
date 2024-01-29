SELECT
    price_date,
    object,
    price,
    min_nights
FROM
    pricing
WHERE
    price_date > CURRENT_DATE - 9
AND price_date <= CURRENT_DATE + 180
ORDER BY
    price_date;