select name, 
        name as coin_name,
        current_price,
        market_cap,
        circulating_supply,
        total_supply,
        last_updated,
        ath,
        atl,
        price_change_percentage_24h_in_currency,
        price_change_percentage_7d_in_currency,
        total_volume,
        high_24h,
        low_24h,
        roi,
        inserted_at,
        case
        when market_cap < 50000000 then 'Very Low Cap'
        when market_cap < 500000000 then 'Low Cap'
        when market_cap < 5000000000 then 'Mid Cap'
        else 'High Cap'
        end as market_cap_usd_bucket,
        DATE_TRUNC('week',inserted_at) as week_start,
        weekofyear(inserted_at) as week_number
        FROM {{ source('coingecko', 'raw_data') }}