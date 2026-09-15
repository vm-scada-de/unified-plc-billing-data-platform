import json
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    dag_id='send_to_email_latest_mp_values',
    start_date=datetime(2026, 1, 1),
    schedule='6 5 * * *',  # Triggers every morning at 08:06 Local Time
    catchup=False,
    tags=['monitoring', 'report', 'mp', 'email']
) as dag:

    # TASK 1: Aggregating latest metrics across channels and building an HTML reporting grid
    def generate_html_table_content(**kwargs):
        pg_hook = PostgresHook(postgres_conn_id='asutp') 

        # Pulling mail delivery distributions directly from the sys_config parameter store
        cfg_query = "SELECT cfg_value FROM public.sys_config WHERE cfg_key = 'report_mp_latest_readings_email';"
        cfg_record = pg_hook.get_first(cfg_query)
        
        cfg_data = cfg_record[0] if cfg_record else {}
        if isinstance(cfg_data, str):
            try:
                cfg_data = json.loads(cfg_data)
            except Exception:
                cfg_data = {}

        # Parsing destination distributions into a comma-delimited recipient array
        recipients_list = cfg_data.get('to', [])
        recipients = ", ".join(recipients_list) if isinstance(recipients_list, list) else str(recipients_list)

        # Enforcing data health check assertions: Fail explicitly if configuration boundaries are absent
        if not recipients:
            raise ValueError("Configuration Error: Key 'to' missing or contains no targets within public.sys_config!")

        subject_template = cfg_data.get('subject', 'Latest Meter Point Telemetry Logs for %Y-%m-%d %H:%M')

        # Capturing interval boundary contexts from operational execution layers
        dt_end = kwargs.get('data_interval_end')

        # Converting contextual evaluation frames explicitly to MSK zone (Europe/Moscow)
        formatted_subject = datetime.now().strftime(subject_template)
        if dt_end and hasattr(dt_end, 'in_timezone'):
            dt_local = dt_end.in_timezone('Europe/Moscow')
            formatted_subject = dt_local.strftime(subject_template)

        # Execution query selecting analytical limits across configured profiles
        query = """
            WITH ranked_logs AS (
                SELECT 
                    mv.dt, 
                    mv.id_mp, 
                    mv.value,
                    ROW_NUMBER() OVER (PARTITION BY mv.id_mp ORDER BY mv.dt DESC) as rn
                FROM public.mp_hourly_values mv
                JOIN public.mp m ON mv.id_mp = m.id
                WHERE m.send_to_email = TRUE AND m.is_active = TRUE
            )
            SELECT 
                rl.dt,
                rl.id_mp,
                rl.value,
                m.name AS device_name,
                m.id_mp_src
            FROM ranked_logs rl
            LEFT JOIN public.mp m ON rl.id_mp = m.id
            WHERE rl.rn = 1
            ORDER BY COALESCE(TRIM(m.name), '');
        """
        
        records = pg_hook.get_records(query)
        rows_html = ""

        # Destructuring explicit element sets returned from internal database adapters
        for dt_raw, log_id, val_data, device_name, mp_src in records:
            
            # Processing potential serialization divergence constraints
            if isinstance(val_data, str):
                try:
                    val_data = json.loads(val_data)
                except Exception:
                    val_data = {}
            
            # Format display timestamps cleanly
            if dt_raw:
                dt_str = dt_raw.strftime('%Y-%m-%d %H:%M') if hasattr(dt_raw, 'strftime') else str(dt_raw)
            else:
                dt_str = "CONNECTION LOST"
            
            # Evaluate target asset names against database properties
            if device_name and str(device_name).strip() != "":
                display_name = str(device_name).strip()
            else:
                display_name = f"Meter Point ID {log_id}"
            
            # Append target connection source origins into structural scopes
            if mp_src and str(mp_src).strip() != "":
                custom_name = f"{display_name} ({str(mp_src).strip()})"
            else:
                custom_name = f"{display_name} (Source Unknown)"
            
            v_plus = 0.0
            v_minus = 0.0
            status_text = "OK"
            badge_style = "background-color: #e2f0d9; color: #385723;"  # Soft Pastel Green
            
            # Accessing operational parameters stored inside structured JSON blocks
            if isinstance(val_data, dict):
                v_plus = val_data.get("value_plus", 0.0)
                v_minus = val_data.get("value_minus", 0.0)
                err_code = val_data.get("code_err", 0)
                
                if err_code != 0:
                    status_text = f"Error ({err_code})"
                    badge_style = "background-color: #fce4d6; color: #c65911;"  # Soft Pastel Orange
            else:
                status_text = "Data Failure"
                badge_style = "background-color: #fce4d6; color: #c65911;"

            # Defensive dynamic cast formatting verification rules
            try:
                v_plus_num = float(v_plus)
                v_minus_num = float(v_minus)
            except (ValueError, TypeError):
                v_plus_num = 0.0
                v_minus_num = 0.0

            minus_color = "#bd2130" if v_minus_num != 0.0 else "#aaaaaa"
            plus_color = "#1e7e34" if v_plus_num != 0.0 else "#555555"

            # Generating static presentation grids via clean CSS inclusions
            rows_html += f"""
            <tr style="border-bottom: 1px solid #e0e0e0;">
                <td style="padding: 6px 8px; color: #555555; font-size: 13px;">{dt_str}</td>
                <td style="padding: 6px 8px; color: #000000; font-size: 13px; font-weight: 500;">{custom_name}</td>
                <td style="padding: 6px 8px; color: {plus_color}; font-size: 13px; text-align: right; font-family: monospace; font-weight: bold;">{v_plus_num:.1f}</td>
                <td style="padding: 6px 8px; color: {minus_color}; font-size: 13px; text-align: right; font-family: monospace;">{v_minus_num:.1f}</td>
                <td style="padding: 6px 8px; font-size: 12px; text-align: center;">
                    <span style="padding: 2px 6px; border-radius: 3px; {badge_style}">{status_text}</span>
                </td>
            </tr>
            """
            
        full_email_body = f"""
        <html>
        <body style="background-color: #ffffff; color: #333333; font-family: Arial, sans-serif; padding: 10px; margin: 0;">
            <div style="max-width: 750px; margin: 0 auto;">
                <div style="border-bottom: 1px solid #cccccc; padding-bottom: 8px; margin-bottom: 15px;">
                    <h4 style="color: #1a365d; margin: 0; font-size: 15px; letter-spacing: 0.5px;">Latest Meter Point Operational Telemetry Reports</h4>
                </div>
                <table style="width: 100%; border-collapse: collapse; text-align: left;">
                    <thead>
                        <tr style="background-color: #f2f4f8; border-bottom: 2px solid #b0c4de;">
                            <th style="color: #4a5568; font-size: 11px; padding: 8px; font-weight: bold; text-transform: uppercase; width: 20%;">Timestamp</th>
                            <th style="color: #4a5568; font-size: 11px; padding: 8px; font-weight: bold; text-transform: uppercase; width: 40%;">Meter Station Endpoint</th>
                            <th style="color: #4a5568; font-size: 11px; padding: 8px; font-weight: bold; text-transform: uppercase; text-align: right; width: 15%;">Forward (+)</th>
                            <th style="color: #4a5568; font-size: 11px; padding: 8px; font-weight: bold; text-transform: uppercase; text-align: right; width: 15%;">Reverse (-)</th>
                            <th style="color: #4a5568; font-size: 11px; padding: 8px; font-weight: bold; text-transform: uppercase; text-align: center; width: 10%;">Operational Status</th>
                        </tr>
                    </thead>
                    <tbody>
                        {rows_html}
                    </tbody>
                </table>
            </div>
        </body>
        </html>
        """

        # Dispatch execution attributes downstream using cross-task XCom interfaces
        kwargs['ti'].xcom_push(key='html_mp_report_body', value=full_email_body)
        kwargs['ti'].xcom_push(key='report_recipients', value=recipients)
        kwargs['ti'].xcom_push(key='report_subject', value=formatted_subject)


    fetch_mp_data_and_build_html = PythonOperator(
        task_id='fetch_mp_data_and_build_html',
        python_callable=generate_html_table_content,
    )

    send_mp_email_report = EmailOperator(
        task_id='send_mp_email_report',
        to="{{ task_instance.xcom_pull(task_ids='fetch_mp_data_and_build_html', key='report_recipients') }}",
        subject="{{ task_instance.xcom_pull(task_ids='fetch_mp_data_and_build_html', key='report_subject') }}",
        html_content="{{ task_instance.xcom_pull(task_ids='fetch_mp_data_and_build_html', key='html_mp_report_body') }}",
        conn_id='smtp_default',
    )

    fetch_mp_data_and_build_html >> send_mp_email_report
