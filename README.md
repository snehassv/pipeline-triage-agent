# pipeline-triage-agent

pipeline-triage-agent/
├── docker-compose.yaml
├── dags/
│   ├── dag_factory.py
│   └── config/pipelines.yaml
├── data/                  # gitignored; seeded locally
├── scripts/
│   ├── seed_warehouse.py
│   └── scenarios/
├── mock_api/              # tiny FastAPI service for the auth-inefficiency DAG
├── agent/                 # YOUR backend
└── ui/                    # YOUR dashboard
