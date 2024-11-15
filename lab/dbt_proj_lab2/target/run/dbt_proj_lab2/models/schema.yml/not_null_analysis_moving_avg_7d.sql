select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select moving_avg_7d
from dev.analytics.analysis
where moving_avg_7d is null



      
    ) dbt_internal_test