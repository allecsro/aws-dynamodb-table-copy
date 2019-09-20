# aws-dynamodb-table-copy
Allows exporting/copying a DynamoDB table using parallel scanning. 
The table can be copying to the same availability zone or to a different zone.

## Install

Before you can begin using the migration script, you should set up authentication credentials. Credentials for your AWS account can be found in the IAM Console. You can create or use an existing user. Go to manage access keys and generate a new set of keys.

If you have the AWS CLI installed, then you can use it to configure your credentials file:

`aws configure`

Alternatively, you can create the credential file yourself. By default, its location is at ~/.aws/credentials:

```
[default]
aws_access_key_id = YOUR_ACCESS_KEY
aws_secret_access_key = YOUR_SECRET_KEY
```

After this you need to install the required python dependencies: 

```
$ pip install -r requirements.txt
```

## Usage

```
usage: ddb-table-copy [-h] -st [SOURCE TABLE] -dt [DEST TABLE] [-p [PROFILE]] [-r [REGION]]
                               [-e [ENDPOINT]] [-s [SEGMENTS]] [-l [LIMIT]]
                               [-no]
```


Example:

```
$ python ddb-table-copy.py --profile=prod --source-table Users --dest-table=Users --segments=5
```


You need to make sure you have properly configured the table read throughput to avoid throttled requests.

A parallel count with a large number of segments can easily consume all of the provisioned throughput for the table. It is best to avoid such counts if the table or index is also incurring heavy read or write activity from other applications.

