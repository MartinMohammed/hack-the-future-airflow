import json
import boto3
from airflow.models import BaseOperator, Variable
from airflow.utils.decorators import apply_defaults

class AwsLambdaTriggerOperator(BaseOperator):
    """
    Custom operator to trigger an AWS Lambda function.
    
    This operator retrieves the Lambda ARN from an Airflow Variable,
    constructs a payload that includes additional environment variables (if any)
    along with the logical date (extracted from the context), and then invokes the Lambda.
    
    :param lambda_variable_key: The Airflow Variable key which stores the Lambda ARN.
    :type lambda_variable_key: str
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
    template_fields = ('env_vars', 'lambda_variable_key')

    @apply_defaults
    def __init__(
        self,
        lambda_variable_key,
        env_vars=None,
        invocation_type='RequestResponse',
        aws_region=None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.lambda_variable_key = lambda_variable_key
        self.env_vars = env_vars or {}
        self.invocation_type = invocation_type
        self.aws_region = aws_region

    def execute(self, context):
        # Retrieve the Lambda ARN from an Airflow Variable
        lambda_arn = Variable.get(self.lambda_variable_key)
        
        # Prepare the payload, starting with any extra environment variables passed in.
        payload = self.env_vars.copy()
        
        # Add the logical date (use 'logical_date' if available, otherwise fall back to 'execution_date')
        if 'logical_date' in context:
            payload['LOGICAL_DATE'] = str(context['logical_date'])
        elif 'execution_date' in context:
            payload['LOGICAL_DATE'] = str(context['execution_date'])
        else:
            self.log.warning("No logical_date or execution_date found in context")

        self.log.info("Invoking Lambda '%s' with payload: %s", lambda_arn, payload)

        # Prepare boto3 client parameters
        client_kwargs = {}
        if self.aws_region:
            client_kwargs['region_name'] = self.aws_region

        lambda_client = boto3.client('lambda', **client_kwargs)

        # Call the Lambda function with the payload
        response = lambda_client.invoke(
            FunctionName=lambda_arn,
            InvocationType=self.invocation_type,
            Payload=json.dumps(payload)
        )

        self.log.info("Lambda function invoked. Response: %s", response)
        return response 