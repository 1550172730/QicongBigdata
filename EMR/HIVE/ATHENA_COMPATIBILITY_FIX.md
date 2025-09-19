# Athena Compatibility Fix for DECIMAL Columns

## Issue Description
The Hive table creation statement worked successfully in Hive but failed in AWS Athena with the following error:

```
GENERIC_INTERNAL_ERROR: class org.apache.hadoop.io.Text cannot be cast to class org.apache.hadoop.hive.serde2.io.HiveDecimalWritable (org.apache.hadoop.io.Text and org.apache.hadoop.hive.serde2.io.HiveDecimalWritable are in unnamed module of loader io.trino.server.PluginClassLoader @21f1e43b)
```

## Root Cause Analysis
The issue was caused by using `OpenCSVSerde` with DECIMAL columns in the `ALLAN_SALE_ODS` table. AWS Athena has stricter type handling compared to Hive, especially when dealing with DECIMAL data types in text format.

### Problematic Configuration (Before Fix):
```sql
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar'='\t')
```

The OpenCSVSerde treats all input data as text initially and then attempts to cast it to the appropriate data types. Athena's implementation is more strict about this casting process, particularly for DECIMAL types, causing the casting failure from Text to HiveDecimalWritable.

## Solution Implemented
Replaced the OpenCSVSerde with LazySimpleSerDe by using `ROW FORMAT DELIMITED`:

### Fixed Configuration (After Fix):
```sql
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
```

## Why This Fix Works
1. **LazySimpleSerDe Compatibility**: The `ROW FORMAT DELIMITED` uses LazySimpleSerDe by default, which has better compatibility between Hive and Athena for type conversions.

2. **Consistent Type Handling**: LazySimpleSerDe handles DECIMAL type conversions more consistently across both Hive and Athena platforms.

3. **Maintained Functionality**: The tab-separated format is preserved, so the data structure and parsing remain the same.

4. **Simplified Configuration**: Removes the complexity of SerDe properties while maintaining the same field separation behavior.

## Affected Tables
- **ALLAN_SALE_ODS**: Modified to use ROW FORMAT DELIMITED
- **Other tables**: No changes needed as they already use compatible formats (Parquet with LazySimpleSerDe)

## DECIMAL Columns in ALLAN_SALE_ODS
The following DECIMAL columns are now properly handled:
- `original_price DECIMAL(10,2)`
- `actual_price DECIMAL(10,2)`
- `discount_amount DECIMAL(10,2)`
- `total_amount DECIMAL(10,2)`
- `shipping_fee DECIMAL(10,2)`
- `tax_amount DECIMAL(10,2)`
- `coupon_amount DECIMAL(10,2)`

## Testing Recommendations
1. Test table creation in both Hive and Athena
2. Verify data insertion and querying works correctly
3. Confirm DECIMAL precision and scale are maintained
4. Test with actual data files to ensure proper parsing

## Best Practices for Hive/Athena Compatibility
1. Use `ROW FORMAT DELIMITED` instead of OpenCSVSerde for text files with DECIMAL columns
2. Store data in Parquet format when possible for better performance and compatibility
3. Test table definitions in both Hive and Athena environments
4. Avoid complex SerDe configurations unless absolutely necessary

## Date: 2025-09-18
## Status: Fixed