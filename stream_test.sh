#!/bin/bash

# Configuration
URL="http://localhost:8080/jobs"
INTERVAL=0.005
COUNT=0

echo "Starting steady stream of jobs to $URL..."
echo "Press [CTRL+C] to stop."

while true
do
    COUNT=$((COUNT+1))
    
    # Send the job
    curl -s -X POST "$URL" \
         -H "Content-Type: application/json" \
         -d "{
               \"type\": \"email\",
               \"payload\": {
                 \"to\": \"stream-$COUNT@example.com\",
                 \"subject\": \"Steady Flow Test #$COUNT\"
               }
             }" > /dev/null

    # Print status to terminal
    echo -ne "Total jobs sent: $COUNT\r"

    # Wait for the next tick
    sleep $INTERVAL
done
