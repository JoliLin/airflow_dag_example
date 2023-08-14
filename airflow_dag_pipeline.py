import pytz
import requests
from datetime import timedelta, datetime

from airflow.decorators import dag, task
from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

def slack_fail_notification(context):
    """
    slack notification when something wrong
    :param context:
    :return:
    """
    failed_alert = SlackWebhookHook(http_conn_id='slack',
                                    attachments=[{
                                        "color": "#ff0000",
                                        "blocks": [{
                                            "type": "section",
                                            "text": {
                                                "type": "mrkdwn",
                                                "text": ":alert: Task Failed."
                                            }
                                        }, {
                                            "type": "section",
                                            "fields": [
                                                {
                                                    "type": "mrkdwn",
                                                    "text": f"*Dag:*\n{context.get('task_instance').dag_id}"
                                                },
                                                {
                                                    "type": "mrkdwn",
                                                    "text": f"*Task:*\n{context.get('task_instance').task_id}"
                                                },
                                                {
                                                    "type": "mrkdwn",
                                                    "text": f"*Execution Time:*\n{context.get('execution_date')}"
                                                },
                                                {
                                                    "type": "mrkdwn",
                                                    "text": f"*Log Url:*\n{context.get('task_instance').log_url}"
                                                },
                                            ]
                                        }]
                                    }],
                                    channel='#logs-ml')

    return failed_alert.execute()


default_args = {
    'owner': 'joli',
    'depends_on_past': False,
    'email': ['joli@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1),
    'on_failure_callback': slack_fail_notification,
}

taiwan_timezone = pytz.timezone('Asia/Taipei')

current_date = datetime.utcnow().date()
yesterday = current_date - timedelta(days=1)
yesterday = yesterday.strftime('%Y-%m-%d')

start_at = datetime.now()
print(start_at)


@dag(
    dag_id='count_daily',
    default_args=default_args,
    description='Count statistic daily',
    schedule_interval=timedelta(hours=12),
    start_date=datetime(2023, 3, 15, 6),
    catchup=False,
    tags=['rXys'],
)
def taskflow():
    @task
    def count_daily(**kwargs):
i       
        ####
        ## API
        ip_address = 'http://0.0.0.0'
        response = requests.get(ip_address + '/api/count_daily').json()
        ####
        
        return response

    @task
    def slack_notification(results, start_at, end_at, **kwargs):
        slack_attachments = [{
            "color": "#00ff00",
            "blocks": [{
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "rXys count statistics"
                }
            }, {
                "type": "section",
                "fields": [{
                    "type": "mrkdwn",
                    "text": f"*Start At:*\n{start_at}"
                }, {
                    "type": "mrkdwn",
                    "text": f"*End At:*\n{end_at}"
                }, {
                    "type": "mrkdwn",
                    "text": f"*#Successful Shops:*\n{results} "
                }]
            }]
        }]

        if '500' in results:
            slack_attachments[0]['color'] = '#ffff00'
            slack_attachments[00]['blocks'][1]['fields'].append({
                "type": "mrkdwn",
                "text": f"*#Fail Shops:*\n{len(results['500'])} shops"
            })
            slack_attachments[00]['blocks'][1]['fields'].append({
                "type": "mrkdwn",
                "text": f"*Fail shops:*\n{results['500']}"
            })

        t = SlackWebhookHook(http_conn_id='slack', attachments=slack_attachments, channel='#logs-ml')

        return t.execute()

    # get current datatime
    end_at = datetime.utcnow().date()
    #start_at = (end_at - timedelta(days=1)).strftime('%Y-%m-%dT%H:00:00')
    end_at = end_at.strftime('%Y-%m-%dT%H:00:00')

    # DAGs
    results = count_daily()
    slack_notification(results=results, start_at=start_at, end_at=end_at)

tasks = taskflow()
