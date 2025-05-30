#!/bin/bash

# === Configuration ===
SLURM_SCRIPT="italy-2d-control.sh"
OUTPUT_NAME="2d_italy_cntrl"
TIMEOUT=240  # seconds
CHECK_INTERVAL=30  # seconds

# === Main loop ===
while true; do
    echo "Submitting SLURM job..."
    JOB_ID=$(sbatch "$SLURM_SCRIPT" | awk '{print $NF}')
    echo "Submitted as job $JOB_ID"

    OUT_FILE="${OUTPUT_NAME}_${JOB_ID}.out"
    ERR_FILE="${OUTPUT_NAME}_${JOB_ID}.err"

    # Wait until files are created
    while [[ ! -f "$ERR_FILE" || ! -f "$OUT_FILE" ]]; do
        sleep 5
    done

    echo "Output files created: $OUT_FILE, $ERR_FILE"

    LAST_ERR_MOD=$(date +%s)

    while squeue -j "$JOB_ID" &> /dev/null; do
        sleep "$CHECK_INTERVAL"

        NEW_ERR_MOD=$(stat -c %Y "$ERR_FILE")
        NEW_OUT_MOD=$(stat -c %Y "$OUT_FILE")

        NOW=$(date +%s)

        if (( NEW_ERR_MOD > LAST_ERR_MOD )); then
            LAST_ERR_MOD=$NEW_ERR_MOD
        elif (( NOW - LAST_ERR_MOD > TIMEOUT )) && (( NEW_OUT_MOD > LAST_ERR_MOD )); then
            echo "Detected stalled job (ERR not updated for $TIMEOUT seconds, but OUT is). Cancelling and resubmitting..."
            scancel "$JOB_ID"
            rm -f "$OUT_FILE" "$ERR_FILE"
            break  # resubmit
        fi
    done

    # Check if job finished (i.e., exited the queue normally)
    if ! squeue -j "$JOB_ID" &> /dev/null; then
        echo "Job $JOB_ID is no longer in the queue."
        echo "Checking job exit code..."
        # Look for successful completion message in .out file
        if grep -q "Successfully finished" "$OUT_FILE"; then
            echo "Job $JOB_ID completed successfully."
            break
        else
            echo "Job $JOB_ID finished but no success message found. You might want to check logs."
            break  # or continue retrying if you want strict success condition
        fi
    fi
done