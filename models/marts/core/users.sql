{{ config(
    materialized='incremental',
    unique_key='user_id'
) }}

SELECT
  user_id,
  name,
  email
FROM `cap-cm-md.testing_dataset.users`
