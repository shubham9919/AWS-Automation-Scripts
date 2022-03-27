import boto3
import time

	# 1. search for the latest job revision - ok
    # 2. check if the compute environment is present - ok 
    #       if not - create one.
	# 2. Get job definition properties and register a job definition - ok
	# 3. check if queue is present - ok
	# 		if not - create queue and goto 4
	# 		if present : goto 4 - ok 
	# 4. get the queue name - ok 
    # 5. Submit every feed job in its corresponding dedicated queue - ok

# Creating a batch job client
client = boto3.client('batch')

def handler(event, context):
    latest_job_revision = getLatestJobRevision()
    compute_env = getComputeEnvironment()
    if(compute_env == False):
        print('compute environment not present, creating new')
        new_compute_env = createComputeEnvironment()
        compute_env = new_compute_env
    job_definition_params = getJobDefinitionProperties(latest_job_revision)
    Jobqueue = isJobQueuePresent()
    if(Jobqueue == False):
        # create a new queue
        print('job queue not present, creating queue')
        create_queue_response = createQueue(compute_env)
        Jobqueue = create_queue_response
    job_definition = registerJobDefinition(job_definition_params)
    time.sleep(3)   # sleeping for 3 sec. queue takes time to get into ok state.
    submit_job = submitJob(Jobqueue, job_definition)
    return submit_job

# Method to get the latest job revision. 
def getLatestJobRevision():

    response = client.describe_job_definitions(    #List all the ACTIVE job definitions
    status='ACTIVE',
    jobDefinitionName='--- JOB DEFINITION NAME ---' 
    )
    # print(type(response))
    print("Got the latest job revision: ")
    print(response['jobDefinitions'][0])
    return response['jobDefinitions'][0]  #Get the latest job definition 

# Method to derrive job definition properties
def getJobDefinitionProperties(latestJobRevision):
    # updatedEnvVar = getUpdatedEnvVariables(latestJobRevision['containerProperties']["environment"])
    containerProps = {
        'jobDefinitionName':'--- JOB DEFINITION NAME ---',
        'type':'container',
        'containerProperties': {
            'image': '--- IMAGE OF APPLICATION TO DEPLOYED OVER SERVER --- ',  #stored in ecr. docker support is also there.
            'vcpus': 4,
            'memory': 512,
            'command': [],
            'jobRoleArn': '--- TASK EXECUTION ROLE ---', #Suggested AmazonECSTaskExecutionRolePolicy - Managed by AWS and If Application is using DynamoDB and other AWS services, add such policies. 
            'volumes': [],
            'environment': [
                '--- ENV VARS TO BE PASSED ---'
            ],
            'mountPoints': [],
            'ulimits': [],
            'resourceRequirements': [],
            'linuxParameters': {},
            'logConfiguration': {},
            'secrets': [],
            "logConfiguration": {
                    "logDriver": "awslogs"
                },
        },
        'retryStrategy':{
                'attempts': 1,
                'evaluateOnExit': []
            },
        'tags' : {
            'test_def': 'automated batch submitting'  #Change as per need
        }
    }

    return containerProps

# Method to get updated environment variables 
# def getUpdatedEnvVariables(envVar):
#     print('update env vars according to use case.')    


# Method to check if job queue is present. 
def isJobQueuePresent(): 
    response = client.describe_job_queues(
    jobQueues=[
        '--- JOB QUEUE NAME ---'
    ],
    nextToken='string'
    )
    if(len(response['jobQueues']) > 0):
        print('Job queue already present : ' + response['jobQueues'][0]['jobQueueName'])
        return response['jobQueues'][0]['jobQueueName'] #we can send jobQueueArn as well 
    else:
        print("Job queue absent, returning False")
        return False

# Method to register job definition. 
def registerJobDefinition(job_definition): 
    print(job_definition)
    response = client.register_job_definition(
        **job_definition
    )
    print("Job definition registered as : " + response['jobDefinitionName'])
    return response['jobDefinitionName'] #can pass the jobDefinitionArn as well. 

# method to submit job.
def submitJob(Jobqueue, job_definition):
    response = client.submit_job(
    jobName='--- BATCH JOB NAME ---',
    jobQueue=Jobqueue,
    dependsOn=[],
    jobDefinition=job_definition
    )
    return response

# Create a new job queue if the job queue is not present. 
def createQueue(compute_env):
    response = client.create_job_queue(
    computeEnvironmentOrder=[
        {
            'computeEnvironment': compute_env,
            'order': 1,
        },
    ],
    jobQueueName='--- QUEUE NAME TO BE CREATED ---',
    priority=1,
    state='ENABLED',
    )
    print("Job queue created : " + response['jobQueueName'])
    return response['jobQueueName']


# Check if the compute environment is present 
def getComputeEnvironment():
    response = client.describe_compute_environments(
    computeEnvironments=[
        '---COMPUTE ENV NAME---',
    ],
    )
    if(len(response['computeEnvironments']) > 0):
        print("Compute environment already present : " + response['computeEnvironments'][0]['computeEnvironmentName'])
        return response['computeEnvironments'][0]['computeEnvironmentName']
    else:
        print("Compute environment absent, returning False.") 
        return False


# create a new compute environment if not present 
def createComputeEnvironment(feed_name):
    response = client.create_compute_environment(
    type='MANAGED',
    computeEnvironmentName='--- NEW COMPUTE ENV NAME ---',
    computeResources={
        'type': 'EC2',
        'desiredvCpus': 0,
        'instanceRole': '--- INSTANCE ROLE ---', # Suggested AmazonEC2ContainerServiceforEC2Role - Managed role by AWS
        'instanceTypes': [
            "optimal"
        ],
        'maxvCpus': 4,    #As per need 
        'minvCpus': 0,
        'securityGroupIds': [
            '--- SG TO BE ASSIGNED TO RESOURCE ---',
        ],
        "subnets": [
            '--- SUBNET-1 ---'
            '--- SUBNET 2 ---' 
        ],
        "ec2Configuration": [
          {
            "imageType": " --- IMAGE TYPE ---" #If not specified, graviton image picked up. 
          }
        ]
    },
    serviceRole='--- SERVICE ROLE ---', # Suggested AWSBatchServiceRole - Managed role by AWS. 
    state='ENABLED',
    )
    print("Compute environment created  : "  + response['computeEnvironmentName'])
    return response['computeEnvironmentName']