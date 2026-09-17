I want you to make parts of an assignment outlined in @assignment.txt. My initial plan is given in @plan.png . This directory already has an ingestion mechanism that
  outputs NDJSON from the financial data. The things I want are 1. a script that inputs the values received from the ingestion script to a Kafka
  topic, simulating a kafka producer 2. a script that takes values from Kafka topics, simulating a consumer 3. a kafka streams component that
  adds a single field to input values that is a string type with value "processed by kafka streams". This is the component that is going to be
  computing the estimated moving averages and buy and sell advisories in the final project. As this is a course assignment, comment on how the
  plan presented in @plan.png can be developed to match the requirements in @assignment.txt

