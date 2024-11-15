
      
  
    

        create or replace transient table dev.snapshot.stock_metrics_snapshot
         as
        (
    

    select *,
        md5(coalesce(cast(date as varchar ), '')
         || '|' || coalesce(cast(date as varchar ), '')
        ) as dbt_scd_id,
        date as dbt_updated_at,
        date as dbt_valid_from,
        
  
  coalesce(nullif(date, date), null)
  as dbt_valid_to

    from (
        



SELECT * FROM dev.analytics.analysis

    ) sbq



        );
      
  
  