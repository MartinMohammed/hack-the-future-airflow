import json
import boto3
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class AwsLambdaTriggerOperator(BaseOperator):
    """
    Custom operator to trigger an AWS Lambda function.
    
    This operator uses the provided Lambda ARN,
    constructs a payload that includes additional environment variables (if any)
    along with the logical date (extracted from the context), and then invokes the Lambda.
    
    :param lambda_arn: The AWS Lambda function ARN.
    :type lambda_arn: str
    :param env_vars: Additional environment variables to pass to the Lambda (optional).
                     For example: {'SOME_KEY': 'some_value'}
    :type env_vars: dict
    :param invocation_type: Lambda invocation type. Default is 'RequestResponse'. 
                            Use 'Event' for asynchronous invocation.
    :type invocation_type: str
    :param aws_region: AWS region name. If not provided, boto3 default region is used.
    :type aws_region: str, optional
    """
    # Allow templating on these fields
    template_fields = ('env_vars', 'lambda_arn')

    @apply_defaults
    def __init__(
        self,
        lambda_arn,
        env_vars=None,
        invocation_type='RequestResponse',
        aws_region=None,
        dag=None,
        *args, **kwargs
    ):
        super().__init__(dag=dag, *args, **kwargs)
        self.lambda_arn = lambda_arn
        self.env_vars = env_vars or {}
        self.invocation_type = invocation_type
        self.aws_region = aws_region

    def execute(self, context):
        # Log context keys for debugging purposes
        self.log.info("Context keys: %s", list(context.keys()))
        
        # Prepare the payload, starting with any extra environment variables passed in.
        payload = self.env_vars.copy()
        
        # Set the current date using context['ds']
        if 'ds' in context:
            current_date = context['ds']
            payload['LOGICAL_DATE'] = current_date
        else:
            self.log.warning("context['ds'] not found in context. 'LOGICAL_DATE' not set.")
        
        self.log.info("Invoking Lambda '%s' with payload: %s", self.lambda_arn, payload)
        
        # Prepare boto3 client parameters
        client_kwargs = {}
        if self.aws_region:
            client_kwargs['region_name'] = self.aws_region
        
        lambda_client = boto3.client('lambda', **client_kwargs)
        
        # Call the Lambda function with the payload
        response = lambda_client.invoke(
            FunctionName=self.lambda_arn,
            InvocationType=self.invocation_type,
            Payload=json.dumps(payload)
        )
        
        self.log.info("Lambda function invoked. Raw response: %s", response)
        
        # Check for a success status code
        if response.get("StatusCode") != 200:
            raise Exception("Lambda invocation failed with status code: {}".format(response.get("StatusCode")))
        
        # Read and decode the response payload to avoid returning unserializable objects.
        payload_response = response['Payload'].read().decode('utf-8')
        self.log.info("Lambda response payload: %s", payload_response)
        return payload_response 