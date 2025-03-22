

WITH monthly_revenue AS (
    SELECT 
        DATE(DATE_TRUNC('month', gp.payment_date)) AS payment_month,
        gp.user_id,
        gp.game_name,
        SUM(gp.revenue_amount_usd) AS total_revenue
    FROM project.games_payments gp
    GROUP BY 1, 2, 3
),
revenue_lag_lead_months AS (
    SELECT *,
        DATE(payment_month - INTERVAL '1 month') AS previous_calendar_month,
        DATE(payment_month + INTERVAL '1 month') AS next_calendar_month,
        LAG(total_revenue) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_paid_month_revenue,
        LAG(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS previous_paid_month,
        LEAD(payment_month) OVER (PARTITION BY user_id ORDER BY payment_month) AS next_paid_month
    FROM monthly_revenue
),
revenue_metrics AS (
    SELECT 
        payment_month,
        user_id,
        game_name,
        total_revenue,
        case
        	when previous_paid_month is null
        	then total_revenue
        end as new_mrr,
        case
        	when next_paid_month is null
        	or next_paid_month != next_calendar_month
        	then next_calendar_month
        end as churn_month,
        case
        	when next_paid_month is null
        	or next_paid_month !=next_calendar_month
        	then total_revenue
        end as churned_revenue,
        case
        	when previous_paid_month = previous_calendar_month
        	    and total_revenue > previous_paid_month_revenue
        	    then total_revenue - previous_paid_month_revenue
        end as expansion_revenue      
    FROM revenue_lag_lead_months
)
SELECT 
    rm.*,
    gpu.language,
    gpu.age,
    gpu.has_older_device_model
    from revenue_metrics rm
    left join project.games_paid_users gpu using(user_id)
