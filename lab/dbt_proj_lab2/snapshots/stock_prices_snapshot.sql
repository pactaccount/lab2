{% snapshot stock_metrics_snapshot %}

{{
  config(
    target_schema='snapshot',
    unique_key='date',
    strategy='timestamp',
    updated_at='date',
    invalidate_hard_deletes=True
  )
}}

SELECT * FROM {{ ref('analysis') }}

{% endsnapshot %}