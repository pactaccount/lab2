
    
    

select
    date as unique_field,
    count(*) as n_records

from dev.analytics.analysis
where date is not null
group by date
having count(*) > 1


