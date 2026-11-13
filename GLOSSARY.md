# AWS Data

A lakehouse built from AWS CloudTrail logs: source logs are mirrored unchanged, then parsed into queryable tables.

## Language

**Log File**:
One `.json.gz` object delivered by CloudTrail to the source bucket, containing many Records.
_Avoid_: File, object, key, log

**Record**:
One JSON object in a Log File's `Records` array, describing a single AWS API activity.
A Record keeps this name in every layer, including as a row in Bronze.
_Avoid_: Event, entry

**Delivery Day**:
The UTC day in a Log File's `yyyy/mm/dd` folder, i.e. when CloudTrail wrote it. It is the partition unit for every layer. A Record's own event time can fall on the previous day (observed for Records in the last minutes of a day), so Delivery Day is not the same as the day the activity happened.
_Avoid_: Event date, partition date

**Raw**:
The layer holding every Log File as an unchanged copy on MinIO, mirrored from the AWS source bucket. It acts as a local-network cache of the source, not as the project's own data store.
_Avoid_: Landing

**Bronze**:
The layer holding every Record the trail delivers, parsed into typed rows of a Delta Lake table. Built only from Raw, never from AWS directly. Silver and gold layers may follow but don't exist yet.

## AWS hierarchy

**Organization**:
The AWS Organization that owns the trail. Its id (always starting `o-`) is the first level of the Log File key layout. The source bucket also holds unrelated folders under `AWSLogs/`, so only folders whose name starts with `o-` count as Organizations.

**Account**:
An AWS account in the Organization. For a Record, the Account is the one the activity happened in (`recipientAccountId`).

**Caller Account**:
The account that owns the identity that made the call (`userIdentity.accountId`). It differs from the Account for cross-account calls.

**Region**:
The AWS region a Log File was delivered for. It is the last level of the key layout before the date.
