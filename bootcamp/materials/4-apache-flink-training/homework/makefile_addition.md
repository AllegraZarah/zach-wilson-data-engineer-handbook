## Included sessionization_job to the Makefile, just below the aggregation_job definition

sessionization_job:
	docker compose exec jobmanager ./bin/flink run -py /opt/src/job/sessionization_job.py --pyFiles /opt/src -d