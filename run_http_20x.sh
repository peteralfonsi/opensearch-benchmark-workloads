for run in {1..20}; do
  echo "STARTING RUN $run \n\n" 
  opensearch-benchmark execute-test --pipeline=benchmark-only --workload-path=/home/ec2-user/osb/opensearch-benchmark-workloads/http_logs --target-host=http://localhost:9200 --throughput-percentiles 0,25,50,75,100 --latency-percentiles 0,10,20,25,30,40,50,60,70,75,80,90,99,99.9,99.99,100 --kill-running-processes
done
