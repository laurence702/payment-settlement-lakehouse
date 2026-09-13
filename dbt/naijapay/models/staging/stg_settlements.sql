with source as (

    select *
    from read_parquet(
        '{{ var("staged_settlements_path") }}',
        hive_partitioning = true,
        union_by_name = true
    )

)

select
    settlement_id,
    transaction_ref,
    merchant_id,
    cast(gross_kobo as bigint) as gross_kobo,
    cast(fee_kobo   as bigint) as fee_kobo,
    cast(net_kobo   as bigint) as net_kobo,
    currency,
    cast(settlement_date as date)      as settlement_date,
    cast(settled_at      as timestamp) as settled_at,
    cast(ingested_at     as timestamp) as ingested_at
from source
