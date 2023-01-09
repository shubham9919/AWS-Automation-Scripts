import json
import boto3
import datetime
import time
import statistics

def lambda_handler(event, context):

    print("event", json.dumps(event))

    lambda_client = boto3.client('lambda')
    cloudwatch = boto3.client('cloudwatch')
    sqs = boto3.client('sqs')
    dynamodb = boto3.resource('dynamodb')


    expected_conc_mapping = {
        'critical_lambda' : 50
    }

    
    data = event["Records"][0]
    data =  {k.lower(): v for k, v in data.items()}
    count = 1
    if data['eventsource'] == 'aws:sns':   
        message = json.loads(event["Records"][0]['Sns']['Message']) # in SNS
        lambda_name = message['Trigger']['Dimensions'][0]['value']
        response = lambda_client.get_function_concurrency(
            FunctionName=lambda_name,
        )
        expected_conc = expected_conc_mapping[lambda_name]
        if 'ReservedConcurrentExecutions' in response:
            print('reseved conc detected')
            prev_reserved_conc = response['ReservedConcurrentExecutions']
            response = lambda_client.put_function_concurrency(
                FunctionName=lambda_name,
                ReservedConcurrentExecutions=expected_conc
            )
        else: 
            print('no reserved conc')
            response = lambda_client.put_function_concurrency(
                FunctionName=lambda_name,
                ReservedConcurrentExecutions=expected_conc
            )
        time.sleep(5)
    elif(data['eventsource'] == 'aws:sqs'):
        message = json.loads(event["Records"][0]['body'])
        lambda_name = message['lambda_name']
        # current_con = message['current_con']
        expected_conc = message['expected_conc']
        prev_reserved_conc = message['prev_reserved_conc'] or prev_reserved_conc
        count = message['count'] if 'count' in message.keys() else count
    else: 
        print('Unknown data source event: ', data)
    # metric data is required in both sqs and sns invocation 
    ## get metric data
    oneday = datetime.timedelta(days=1)
    yesterday = datetime.date.today() - oneday
    future_date = datetime.date.today() + 6*oneday
    current_metric = cloudwatch.get_metric_data( 
    MetricDataQueries=[
        {
            'Id': 'concurrent_exe',
            'MetricStat': {
                'Metric': {
                  "Namespace": "AWS/Lambda",
                  "MetricName": "ConcurrentExecutions",
                  "Dimensions": [
                    {
                      "Name": "FunctionName",
                      "Value": lambda_name
                    }
                  ]
                },
                'Period': 60,
                'Stat': 'Maximum',
            },
        },
    ],
    StartTime=datetime.datetime(yesterday.year, yesterday.month, yesterday.day),
    EndTime=datetime.datetime(future_date.year, future_date.month, future_date.day)
    )

    # get average metrics data
    # prev_months = datetime.date.today() - 3*30*oneday
    # avg_metric = cloudwatch.get_metric_data(
    # MetricDataQueries=[
    #     {
    #         'Id': 'concurrent_exe',
    #         'MetricStat': {
    #             'Metric': {
    #               "Namespace": "AWS/Lambda",
    #               "MetricName": "ConcurrentExecutions",
    #               "Dimensions": [
    #                 {
    #                   "Name": "FunctionName",
    #                   "Value": lambda_name
    #                 }
    #               ]
    #             },
    #             'Period': 86400,
    #             'Stat': 'Average',
    #         },
    #     },
    # ],
    # StartTime=datetime.datetime(prev_months.year, prev_months.month, prev_months.day),
    # EndTime=datetime.datetime(yesterday.year, yesterday.month, yesterday.day)
    # )

    # avg_conc = round(statistics.mean(avg_metric["MetricDataResults"][0]['Values']))
    current_con =round(current_metric["MetricDataResults"][0]['Values'][0])


    if current_con != expected_conc:
        count = 1 # reset the sqs termination counter to 0 
        if current_con < prev_reserved_conc:
            # reinstate to prev_reserved_conc
            print('reverting to original concurancy')
            response = lambda_client.put_function_concurrency(
                FunctionName=lambda_name,
                ReservedConcurrentExecutions=prev_reserved_conc
            )
            # no sqs publish 
        else:
            # make the func conc to current_con + 2
            print('Updating func concurraccncy to: ', current_con + 2)
            response = lambda_client.put_function_concurrency(
                FunctionName=lambda_name,
                ReservedConcurrentExecutions=current_con + 2
            )
            # publish sqs with 
                #prev_reserved_conc as og 
                #expected_conc as current_con
                #lambda_name
            print('publishing to sqs')
            res = sqs.send_message(
                QueueUrl = 'https://sqs.us-east-1.amazonaws.com/366970407952/poc_queue',
                MessageBody = json.dumps({
                    'lambda_name': lambda_name,
                    'prev_reserved_conc': prev_reserved_conc,
                    'expected_conc': current_con,
                    'count': count
                }),
            DelaySeconds = 60
            )
    else:
        # in case the consurrancy is constant from lst several minutes, terminate te sqs publish and wait for the another alert
        if count >= 3: #if the conc is constant over 2 minutes 
            print('max count passed, terminating sqs publishinh. Count: ', count)
        else:
            count = count + 1
            print('In the Else block, publishing to sqs: ', json.dumps({
                        'lambda_name': lambda_name,
                        'prev_reserved_conc': prev_reserved_conc,
                        'expected_conc': current_con,
                        'count': count
                    }))

            res = sqs.send_message(
                    QueueUrl = 'https://sqs.us-east-1.amazonaws.com/366970407952/poc_queue',
                    MessageBody = json.dumps({
                        'lambda_name': lambda_name,
                        'prev_reserved_conc': prev_reserved_conc,
                        'expected_conc': current_con,
                        'count': count,
                    }),
                DelaySeconds = 60
                )
    table = dynamodb.Table("poc")
    dynamodb = boto3.client('dynamodb')
    ddb_item = dynamodb.get_item(
    TableName='poc', 
    Key={'lambda_name':{
        'S':str(lambda_name)
        }
    })
    print('ddb_item', ddb_item)
    reserved_conc_list = []
    if 'Item' in ddb_item.keys():
        reserved_conc_list = ddb_item["Item"]['updated_reserved_conc']
        result = table.update_item(
            Key={
                'lambda_name': lambda_name
            },
            UpdateExpression="SET updated_reserved_conc = list_append(updated_reserved_conc, :i)",
            ExpressionAttributeValues={
                ':i': [expected_conc+2],
            },
            ReturnValues="UPDATED_NEW"
        )
        print('DDB update:', result)
    else:
        reserved_conc_list.append(expected_conc)
        table.put_item(Item= {
            'lambda_name': lambda_name,
            'prev_conc': prev_reserved_conc,
            'updated_reserved_conc': reserved_conc_list
    })

    res  = {
        # 'avg_metric': avg_metric,
        'current_metric': current_metric,
        # 'avg_conc': avg_conc
    }
    
    print(json.dumps(res, default=str))
    return {
        'statusCode': 200,
        'body': json.dumps(res, default=str)
    }
    