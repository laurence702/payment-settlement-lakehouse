-- One row per transaction_ref. Spark already collapsed the event stream and
-- removed duplicates, so this layer only renames, casts and filters. Doing
-- real work here would mean two places own deduplication.

with source as (

    select *
    from read_parquet(
        '{{ var("staged_transactions_path") }}',
        hive_partitioning = true,
        union_by_name = true
    )

),

renamed as (

    select
        transaction_ref,
        merchant_id,
        customer_id,
        gateway,
        channel,
        bank_code,
        status,
        currency,
        failure_reason,

        -- Money stays in integer kobo end to end. The naira conversion is done
        -- once, at the presentation edge, never in an aggregate.
        cast(amount_kobo      as bigint) as amount_kobo,
        cast(fee_kobo         as bigint) as fee_kobo,
        cast(amount_ngn_kobo  as bigint) as amount_ngn_kobo,
        cast(fee_ngn_kobo     as bigint) as fee_ngn_kobo,
        cast(fx_rate_to_ngn   as double) as fx_rate_to_ngn,

        cast(created_at   as timestamp) as created_at,
        cast(updated_at   as timestamp) as updated_at,
        cast(ingested_at  as timestamp) as ingested_at,
        cast(event_date   as date)      as event_date,
        cast(arrival_lag_seconds as bigint) as arrival_lag_seconds,

        is_terminal,
        status = 'success'  as is_success,
        status = 'reversed' as is_reversed,

        -- Eligible for settlement: succeeded and was not subsequently reversed.
        -- This predicate is the definition the reconciliation mart depends on,
        -- so it is declared once here.
        (status = 'success') as is_settlement_eligible

    from source

)

select * from renamed
