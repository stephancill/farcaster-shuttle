#!/bin/bash

# Define concurrency levels to test
concurrency_levels=(4 4 4 8 8 8 16 16 16)

# Start the CSV file and add the header row
echo "Concurrency,Total Time,Rows" > benchmark_results.csv

# Loop over each concurrency level
for CONCURRENCY in "${concurrency_levels[@]}"; do
  echo "Testing with concurrency: $CONCURRENCY"
  
  # Run the docker-compose commands and capture the output
  output=$(docker-compose down -v && docker-compose up -d && MAX_FID=1000 CONCURRENCY=$CONCURRENCY yarn start bench-backfill)
  
  # Extract the total time and rows from the output
  total_time=$(echo "$output" | grep "\[benchmark complete\]" | awk '{print $6}' | cut -d',' -f1)
  rows=$(echo "$output" | grep "\[benchmark complete\]" | awk '{print $6}' | cut -d',' -f2)

  echo "Total time: $total_time"
  echo "Rows: $rows"

  
  # Add the results to the CSV file
  echo "$CONCURRENCY,$total_time,$rows" >> benchmark_results.csv
done

echo "Benchmarking complete. Results saved to benchmark_results.csv"