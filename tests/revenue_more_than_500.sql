select
    revenue
from {{ref ('dim_transaction_revenue') }}
    where revenue > 500