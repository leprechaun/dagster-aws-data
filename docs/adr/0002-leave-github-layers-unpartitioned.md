# Leave the GitHub Raw and Bronze layers unpartitioned

CloudTrail and CloudFront are partitioned by Delivery Day and Request Day, but the GitHub Raw and Bronze layers are not. GitHub volume is tiny: about 5k files and 14k deliveries, under 200 MB in total. A Raw run lists each event type's folder anyway, so a partition saves no listing work. Raw mirror is idempotent, so a run can copy every new file without knowing which days changed. Partitions over 655 days times 17 event types (about 11k partitions) added bookkeeping and saved no work. Bronze is rebuilt in full per event type, which is cheap at this size.

So each GitHub Raw asset is one unpartitioned run per tick that copies whatever Raw doesn't have yet, and each Bronze table is one unpartitioned table per event type. CloudTrail stays partitioned, since its volume makes per-day runs and rebuilds worthwhile.
