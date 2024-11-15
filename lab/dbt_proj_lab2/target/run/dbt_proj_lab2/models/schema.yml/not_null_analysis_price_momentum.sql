select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select price_momentum
from dev.analytics.analysis
where price_momentum is null



      
    ) dbt_internal_test