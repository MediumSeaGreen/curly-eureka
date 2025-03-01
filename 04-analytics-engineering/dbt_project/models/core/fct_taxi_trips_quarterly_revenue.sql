with quarterly_revenue as (
    select
        extract(quarter from pickup_datetime) as quarter,
        extract(year from pickup_datetime) as year,
        service_type,
        sum(total_amount) as total_revenue
    from {{ ref('fact_trips') }}
    group by quarter, year, service_type
),
quarterly_revenue_with_growth as (
    select
        quarter,
        year,
        service_type,
        total_revenue,
        case 
            when lag(total_revenue) over (partition by service_type, quarter order by year) = 0 then null
            else (total_revenue - lag(total_revenue) over (partition by service_type, quarter order by year)) / lag(total_revenue) over (partition by service_type, quarter order by year) * 100 
        end as yoy_growth
    from quarterly_revenue
)

select * from quarterly_revenue_with_growth
where year = 2020
order by service_type, yoy_growth