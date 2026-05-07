#!/bin/bash

# =============================================================================
# MDM Test Data Upload Script
# =============================================================================
# 
# This script uploads generated test data to the appropriate Snowflake stages
# based on the MDM directory structure:
#   output/initial/A/customer/  -> CRMI_RAW_ST_CUSTOMER_A
#   output/initial/A/address/   -> CRMI_RAW_ST_ADDRESSES_A
#   output/initial/B/customer/  -> CRMI_RAW_ST_CUSTOMER_B
#   output/initial/B/address/   -> CRMI_RAW_ST_ADDRESSES_B
#   output/update/A/customer/   -> CRMI_RAW_ST_CUSTOMER_A
#   output/update/A/address/    -> CRMI_RAW_ST_ADDRESSES_A
#   output/update/B/customer/   -> CRMI_RAW_ST_CUSTOMER_B
#   output/update/B/address/    -> CRMI_RAW_ST_ADDRESSES_B
#
# Usage:
#   ./upload_data.sh --CONNECTION_NAME=<my-sf-connection> [OPTIONS]
#
# Options:
#   --CONNECTION_NAME=<name>  Snowflake connection name (required)
#   --DATABASE=<name>         Override database name (default: MASTER_DATA_MANAGEMENT)
#   --INITIAL_ONLY            Upload only initial load data
#   --UPDATES_ONLY            Upload only update data
#   --DRY_RUN                 Test run without uploading
#
# Examples:
#   ./upload_data.sh --CONNECTION_NAME=mdm_dev
#   ./upload_data.sh --CONNECTION_NAME=mdm_dev --DRY_RUN
#   ./upload_data.sh --CONNECTION_NAME=mdm_dev --INITIAL_ONLY
# =============================================================================

set -e

# --- Default values ---
BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="$BASE_DIR/output"
DATABASE="MDM_DEV"
SCHEMA="MDM_RAW_001"
CONNECTION_NAME=""
DRY_RUN=false
INITIAL_ONLY=false
UPDATES_ONLY=false

# --- Tracking variables ---
TOTAL_UPLOADS=0
SUCCESSFUL_UPLOADS=0
FAILED_UPLOADS=0

# --- Parse arguments ---
for ARG in "$@"; do
  case $ARG in
    --DATABASE=*)
      DATABASE="${ARG#*=}"
      ;;
    --CONNECTION_NAME=*)
      CONNECTION_NAME="${ARG#*=}"
      ;;
    --INITIAL_ONLY)
      INITIAL_ONLY=true
      ;;
    --UPDATES_ONLY)
      UPDATES_ONLY=true
      ;;
    --DRY_RUN)
      DRY_RUN=true
      ;;
    *)
      echo "[ERROR] Unknown argument: $ARG"
      echo "Usage: $0 --CONNECTION_NAME=<name> [OPTIONS]"
      exit 1
      ;;
  esac
done

# --- Validate required inputs ---
if [[ -z "$CONNECTION_NAME" ]]; then
  echo "[ERROR] Missing required argument: --CONNECTION_NAME"
  echo ""
  echo "Usage: $0 --CONNECTION_NAME=<name> [OPTIONS]"
  echo ""
  echo "Options:"
  echo "  --CONNECTION_NAME=<name>  Snowflake connection name (required)"
  echo "  --DATABASE=<name>         Override database name (default: MASTER_DATA_MANAGEMENT)"
  echo "  --INITIAL_ONLY            Upload only initial load data"
  echo "  --UPDATES_ONLY            Upload only update data"
  echo "  --DRY_RUN                 Test run without uploading"
  exit 1
fi

# --- Validate data directory ---
if [[ ! -d "$DATA_DIR" ]]; then
  echo "[ERROR] Data directory not found: $DATA_DIR"
  echo ""
  echo "Please generate test data first:"
  echo "  cd scripts && python3 generate_test_data.py"
  exit 1
fi

echo "================================================================"
echo "=== MDM Test Data Upload"
echo "================================================================"
echo "Data Directory: $DATA_DIR"
echo "Database: $DATABASE"
echo "Schema: $SCHEMA"
echo "Connection: $CONNECTION_NAME"
echo "Dry Run: $DRY_RUN"
if [[ "$INITIAL_ONLY" == "true" ]]; then
  echo "Mode: Initial data only"
elif [[ "$UPDATES_ONLY" == "true" ]]; then
  echo "Mode: Update data only"
else
  echo "Mode: All data (initial + updates)"
fi
echo ""

# --- Test connection ---
if [[ "$DRY_RUN" != "true" ]]; then
  echo "Testing Snowflake connection..."
  set +e
  snow sql -c "$CONNECTION_NAME" -q "SELECT CURRENT_VERSION();" > /dev/null 2>&1
  if [[ $? -ne 0 ]]; then
    echo "[ERROR] Failed to connect to Snowflake"
    echo "Please verify your connection: snow connection test -c $CONNECTION_NAME"
    exit 1
  fi
  set -e
  echo "[OK] Connection successful!"
  echo ""
fi

# --- Function to upload files to stage ---
upload_to_stage() {
  local source_dir="$1"
  local stage_name="$2"
  local description="$3"
  
  # Check if directory exists and has files
  if [[ ! -d "$source_dir" ]]; then
    echo "  [SKIP] Directory not found: $source_dir"
    return 0
  fi
  
  local file_count=$(find "$source_dir" -name "*.csv" -type f 2>/dev/null | wc -l | tr -d ' ')
  
  if [[ $file_count -eq 0 ]]; then
    echo "  [SKIP] No CSV files in: $source_dir"
    return 0
  fi
  
  TOTAL_UPLOADS=$((TOTAL_UPLOADS + 1))
  
  echo "  Uploading: $description"
  echo "    Source: $source_dir"
  echo "    Stage: $stage_name"
  echo "    Files: $file_count"
  
  if [[ "$DRY_RUN" == "true" ]]; then
    echo "    [DRY RUN] Would upload $file_count files"
    SUCCESSFUL_UPLOADS=$((SUCCESSFUL_UPLOADS + 1))
    return 0
  fi
  
  # Build SQL command
  local sql_command="
    USE DATABASE $DATABASE;
    USE SCHEMA $SCHEMA;
    PUT file://$source_dir/*.csv @$stage_name AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
  "
  
  set +e
  snow sql -c "$CONNECTION_NAME" -q "$sql_command" > /tmp/mdm_upload_output.txt 2>&1
  local result=$?
  set -e
  
  if [[ $result -eq 0 ]]; then
    echo "    [OK] Uploaded $file_count files"
    SUCCESSFUL_UPLOADS=$((SUCCESSFUL_UPLOADS + 1))
  else
    echo "    [FAIL] Upload failed"
    cat /tmp/mdm_upload_output.txt | head -10 | sed 's/^/    /'
    FAILED_UPLOADS=$((FAILED_UPLOADS + 1))
  fi
  
  echo ""
}

# =============================================================================
# UPLOAD INITIAL DATA
# =============================================================================
if [[ "$UPDATES_ONLY" != "true" ]]; then
  echo "================================================================"
  echo "=== INITIAL LOAD DATA"
  echo "================================================================"
  echo ""
  
  upload_to_stage \
    "$DATA_DIR/initial/A/customer" \
    "CRMI_RAW_ST_CUSTOMER_A" \
    "CRM A Customers (Initial)"
  
  upload_to_stage \
    "$DATA_DIR/initial/A/address" \
    "CRMI_RAW_ST_ADDRESSES_A" \
    "CRM A Addresses (Initial)"
  
  upload_to_stage \
    "$DATA_DIR/initial/B/customer" \
    "CRMI_RAW_ST_CUSTOMER_B" \
    "CRM B Customers (Initial)"
  
  upload_to_stage \
    "$DATA_DIR/initial/B/address" \
    "CRMI_RAW_ST_ADDRESSES_B" \
    "CRM B Addresses (Initial)"
  
  upload_to_stage \
    "$DATA_DIR/initial/C/customer" \
    "CRMI_RAW_ST_CUSTOMER_C" \
    "CRM C Customers (Initial)"
  
  upload_to_stage \
    "$DATA_DIR/initial/C/address" \
    "CRMI_RAW_ST_ADDRESSES_C" \
    "CRM C Addresses (Initial)"
fi

# =============================================================================
# UPLOAD UPDATE DATA
# =============================================================================
if [[ "$INITIAL_ONLY" != "true" ]]; then
  echo "================================================================"
  echo "=== UPDATE DATA (SCD Type 2)"
  echo "================================================================"
  echo ""
  
  upload_to_stage \
    "$DATA_DIR/update/A/customer" \
    "CRMI_RAW_ST_CUSTOMER_A" \
    "CRM A Customers (Updates)"
  
  upload_to_stage \
    "$DATA_DIR/update/A/address" \
    "CRMI_RAW_ST_ADDRESSES_A" \
    "CRM A Addresses (Updates)"
  
  upload_to_stage \
    "$DATA_DIR/update/B/customer" \
    "CRMI_RAW_ST_CUSTOMER_B" \
    "CRM B Customers (Updates)"
  
  upload_to_stage \
    "$DATA_DIR/update/B/address" \
    "CRMI_RAW_ST_ADDRESSES_B" \
    "CRM B Addresses (Updates)"
  
  upload_to_stage \
    "$DATA_DIR/update/C/customer" \
    "CRMI_RAW_ST_CUSTOMER_C" \
    "CRM C Customers (Updates)"
  
  upload_to_stage \
    "$DATA_DIR/update/C/address" \
    "CRMI_RAW_ST_ADDRESSES_C" \
    "CRM C Addresses (Updates)"
fi

# =============================================================================
# REFRESH STREAMS
# =============================================================================
if [[ "$DRY_RUN" != "true" && $SUCCESSFUL_UPLOADS -gt 0 ]]; then
  echo "================================================================"
  echo "=== REFRESHING STAGE METADATA"
  echo "================================================================"
  echo ""
  echo "Triggering directory table refresh for stream detection..."
  
  snow sql -c "$CONNECTION_NAME" -q "
    USE DATABASE $DATABASE;
    USE SCHEMA $SCHEMA;
    ALTER STAGE CRMI_RAW_ST_CUSTOMER_A REFRESH;
    ALTER STAGE CRMI_RAW_ST_CUSTOMER_B REFRESH;
    ALTER STAGE CRMI_RAW_ST_CUSTOMER_C REFRESH;
    ALTER STAGE CRMI_RAW_ST_ADDRESSES_A REFRESH;
    ALTER STAGE CRMI_RAW_ST_ADDRESSES_B REFRESH;
    ALTER STAGE CRMI_RAW_ST_ADDRESSES_C REFRESH;
  " > /dev/null 2>&1 || echo "  [WARN] Stage refresh may require additional permissions"
  
  echo "[OK] Stage metadata refreshed"
  echo ""
fi

# =============================================================================
# UPLOAD SUMMARY
# =============================================================================
echo "================================================================"
echo "=== UPLOAD SUMMARY"
echo "================================================================"
echo ""
echo "Total upload operations: $TOTAL_UPLOADS"
echo "Successful: $SUCCESSFUL_UPLOADS"
echo "Failed: $FAILED_UPLOADS"
echo ""

if [[ "$DRY_RUN" == "true" ]]; then
  echo "[DRY RUN] Completed - no files were uploaded"
  echo ""
  echo "To execute the actual upload, run:"
  echo "  $0 --CONNECTION_NAME=$CONNECTION_NAME"
elif [[ $FAILED_UPLOADS -eq 0 && $SUCCESSFUL_UPLOADS -gt 0 ]]; then
  echo "[SUCCESS] All uploads completed successfully!"
  echo ""
  echo "Next steps:"
  echo "  1. Check streams have data:"
  echo "     SELECT SYSTEM\$STREAM_HAS_DATA('$DATABASE.$SCHEMA.CRMI_RAW_SM_CUSTOMER_A');"
  echo ""
  echo "  2. Enable tasks to start loading:"
  echo "     ALTER TASK $DATABASE.$SCHEMA.CRMI_RAW_TS_LOAD_CUSTOMER_A RESUME;"
  echo "     ALTER TASK $DATABASE.$SCHEMA.CRMI_RAW_TS_LOAD_CUSTOMER_B RESUME;"
  echo "     ALTER TASK $DATABASE.$SCHEMA.CRMI_RAW_TS_LOAD_ADDRESSES_A RESUME;"
  echo "     ALTER TASK $DATABASE.$SCHEMA.CRMI_RAW_TS_LOAD_ADDRESSES_B RESUME;"
  echo ""
  echo "  3. Monitor task execution:"
  echo "     SELECT * FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())"
  echo "     WHERE DATABASE_NAME = '$DATABASE' ORDER BY SCHEDULED_TIME DESC;"
  echo ""
  echo "  4. Verify data loaded:"
  echo "     SELECT COUNT(*) FROM $DATABASE.$SCHEMA.CRMI_RAW_TB_CUSTOMER_A;"
elif [[ $SUCCESSFUL_UPLOADS -eq 0 ]]; then
  echo "[WARN] No files were uploaded"
  echo "Please ensure test data has been generated:"
  echo "  cd scripts && python3 generate_test_data.py"
else
  echo "[WARN] Upload completed with some errors"
  echo "Please review the failed uploads above"
fi

echo ""
echo "Upload process completed!"
