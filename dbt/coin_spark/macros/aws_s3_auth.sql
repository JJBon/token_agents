{% macro set_s3a_session_creds() %}
  {% set ak = env_var('AWS_ACCESS_KEY_ID') %}
  {% set sk = env_var('AWS_SECRET_ACCESS_KEY') %}
  {% set st = env_var('AWS_SESSION_TOKEN') %}
  {% set region = env_var('AWS_REGION', 'us-east-1') %}

  {% if ak and sk and st %}
    {% do run_query("SET spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") %}
    {% do run_query("SET spark.hadoop.fs.s3a.access.key=" ~ ak) %}
    {% do run_query("SET spark.hadoop.fs.s3a.secret.key=" ~ sk) %}
    {% do run_query("SET spark.hadoop.fs.s3a.session.token=" ~ st) %}
    {% do run_query("SET spark.hadoop.fs.s3a.endpoint=s3." ~ region ~ ".amazonaws.com") %}
  {% endif %}
{% endmacro %}