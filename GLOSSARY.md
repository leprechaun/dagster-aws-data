# AWS Data

A lakehouse built from AWS CloudTrail and CloudFront logs: source logs are mirrored unchanged, then parsed into queryable tables.

## Language

**Log File**:
One gzip object delivered by an AWS logging service to a source bucket: a `.json.gz` of Records (CloudTrail) or a `.gz` of Requests (CloudFront).
_Avoid_: File, object, key, log

**Record**:
One JSON object in a Log File's `Records` array, describing a single AWS API activity.
A Record keeps this name in every layer, including as a row in Bronze.
_Avoid_: Event, entry

**Delivery Day**:
The UTC day in a Log File's `yyyy/mm/dd` folder, i.e. when CloudTrail wrote it. It is the partition unit for every CloudTrail layer. A Record's own event time can fall on the previous day (observed for Records in the last minutes of a day), so Delivery Day is not the same as the day the activity happened.
_Avoid_: Event date, partition date

**Request**:
One line of a CloudFront Log File: a single viewer request to a Distribution. A Request keeps this name in every layer, including as a row in Bronze.
_Avoid_: Record (CloudTrail's term), hit, event

**Request Day**:
The UTC day in a CloudFront Log File's `yyyy/mm/dd` folder, which is the day its Requests were made (seen on one day of data; the midnight boundary is not yet checked). It is the partition unit for every CloudFront layer.
_Avoid_: Delivery Day (CloudTrail's term), log date

**Raw**:
The layer holding every Log File as an unchanged copy on MinIO, mirrored from the AWS source bucket. It acts as a local-network cache of the source, not as the project's own data store.
_Avoid_: Landing

**Bronze**:
The layer holding every Record or Request delivered, parsed into typed rows of a Delta Lake table. Built only from Raw, never from AWS directly. A silver layer may follow but doesn't exist yet.

**Gold**:
The layer holding tables built from Bronze for analysis, rather than one row per Record or Request. Today it holds the CloudTrail Activity table, rebuilt in full from Bronze each hour.

**Activity**:
Records counted per time bucket, by caller, API, access (read-only or a change), account, region and error, at several grains (15 minutes, an hour, a day) in one Gold table told apart by a `grain` column. Counts add up within a grain, never across grains. A key a grain leaves out reads `(all)`; a key missing in the source reads `(none)`.
_Avoid_: Aggregate, rollup

## AWS hierarchy

**Organization**:
The AWS Organization that owns the trail. Its id (always starting `o-`) is the first level of the Log File key layout. The source bucket also holds unrelated folders under `AWSLogs/`, so only folders whose name starts with `o-` count as Organizations.

**Account**:
An AWS account that delivers logs to a source bucket. For a CloudTrail Record, the Account is the one the activity happened in (`recipientAccountId`); for CloudFront, it is the one that owns the Distribution. Any 12-digit folder under `AWSLogs/` counts as an Account.

**Caller Account**:
The account that owns the identity that made the call (`userIdentity.accountId`). It differs from the Account for cross-account calls.

**Region**:
The AWS region a Log File was delivered for. It is the last level of the key layout before the date.

## CloudFront

**Distribution**:
A CloudFront distribution, identified by an id like `E1EXAMPLE0001`. Its Log Files sit under its own folder, beneath the Account that owns it.
_Avoid_: Site, CDN
