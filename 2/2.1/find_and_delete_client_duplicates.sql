BEGIN;

WITH duplicate_keys AS (
    SELECT
        client_rk,
        effective_from_date,
        count(*) AS rows_count
    FROM dm.client
    GROUP BY
        client_rk,
        effective_from_date
    HAVING count(*) > 1
)
SELECT
    client_rk,
    effective_from_date,
    rows_count
FROM duplicate_keys
ORDER BY
    client_rk,
    effective_from_date;

WITH ranked AS (
    SELECT
        ctid,
        row_number() OVER (
            PARTITION BY client_rk, effective_from_date
            ORDER BY ctid
        ) AS rn
    FROM dm.client
),
deleted AS (
    DELETE FROM dm.client AS c
    USING ranked AS r
    WHERE c.ctid = r.ctid
      AND r.rn > 1
    RETURNING
        c.*
)
SELECT
    *
FROM deleted
ORDER BY
    client_rk,
    effective_from_date,
    effective_to_date NULLS LAST,
    client_open_dttm NULLS LAST;

CREATE UNIQUE INDEX IF NOT EXISTS ux_dm_client_client_rk_effective_from_date
    ON dm.client (client_rk, effective_from_date);

SELECT count(*) AS duplicate_keys_after_cleanup
FROM (
    SELECT
        client_rk,
        effective_from_date
    FROM dm.client
    GROUP BY
        client_rk,
        effective_from_date
    HAVING count(*) > 1
) AS d;

COMMIT;
