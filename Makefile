PYTHON ?= python
PIP ?= $(PYTHON) -m pip
PYTEST ?= PYTHONPATH=src $(PYTHON) -m pytest
TORCH_INDEX_URL ?= https://download.pytorch.org/whl/cpu
DATASET_DIR ?= samples/button_press_001
RUN_DIR ?= runs/button_press_001
K8S_ARTIFACT_DIR ?= runs/k8s-button_press_001
SCAN_OUTPUT_ROOT ?= runs/scan
K8S_SCAN_JOB_NAME ?= robot-dh-scan-manual

# v1.4 数据湖本地路径（可通过变量覆盖）
LAKE_ROOT_LOCAL ?= runs/lake
LAKE_DATASET_ID ?= button_press_001
LAKE_DATASET_VERSION ?= v1
LAKE_ODS_LOCAL ?= $(LAKE_ROOT_LOCAL)/ods/$(LAKE_DATASET_ID)/$(LAKE_DATASET_VERSION)
LAKE_DWD_LOCAL ?= $(LAKE_ROOT_LOCAL)/dwd/$(LAKE_DATASET_ID)/$(LAKE_DATASET_VERSION)
LAKE_ADS_LOCAL ?= $(LAKE_ROOT_LOCAL)/ads/quality
LAKE_BUCKET_REMOTE ?= robot-lake
LAKE_ROOT_REMOTE ?= s3://$(LAKE_BUCKET_REMOTE)/
DATASETS_BUCKET_REMOTE ?= robot-datasets

.PHONY: setup test demo-data demo-local scan-local infra-doctor infra-doctor-json \
        docker-build kind-load \
        k8s-apply k8s-apply-cronjob k8s-apply-remote-secret k8s-status k8s-copy-data \
        k8s-run-job k8s-run-job-remote k8s-logs k8s-copy-artifacts k8s-api-port-forward \
        k8s-run-scan-once k8s-run-scan-remote k8s-scan-logs \
        clean-runs k8s-clean \
        lake-doctor lake-list lake-audit \
        normalize-demo-local features-demo-local ads-demo-local etl-demo-local \
        etl-remote-one etl-remote-scan k8s-run-etl-remote \
        k8s-apply-lake-secret-example k8s-apply-lake k8s-lake-doctor \
        k8s-run-etl-one k8s-run-etl-scan k8s-run-build-ads \
        k8s-lake-logs k8s-apply-lake-cron k8s-lake-status k8s-delete-lake-jobs \
        benchmark-local etl-plan-scale30 etl-run-shard-0 etl-merge-scale30 perf-query v1-5-smoke \
        argo-install argo-status argo-ui-port-forward argo-apply-rbac argo-apply-templates \
        argo-submit-scale-etl argo-submit-benchmark argo-submit-build-ads argo-apply-cron \
        argo-list argo-logs argo-delete-completed k8s-apply-v1-5-secret-example \
        argo-sync-log-archive-secret argo-apply-log-archive \
        argo-verify-log-archive argo-enable-log-archive \
	exporter-build exporter-test exporter-run exporter-docker-build exporter-kind-load \
        exporter-k8s-apply exporter-port-forward exporter-logs \
        local-preflight local-init-data local-mc-alias local-plan-devscale \
        local-sync-devscale local-verify-devscale local-create-kind-dev \
        local-destroy-kind-dev local-kind-status local-apply-data-pvc \
        local-data-debug local-devscale-summary local-print-dev-env \
        local-runtime-doctor local-datasets-list local-datasets-verify \
        local-adapter-list local-adapter-detect local-adapter-probe \
        local-qc-devscale local-etl-devscale local-ml-ready-devscale \
        local-heartbeat-check local-argo-logs-index v1-7-local-smoke \
        argo-local-apply argo-local-submit argo-local-submit-qc \
        argo-local-submit-ml-ready argo-local-tail argo-local-status \
        argo-local-logs argo-local-sync argo-local-debug \
        argo-local-clean-completed v1-7-local-platform-smoke

setup:
	$(PIP) install --upgrade pip
	$(PIP) install --index-url $(TORCH_INDEX_URL) torch
	$(PIP) install -e .[dev]

test:
	$(PYTEST)

demo-data:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli generate-demo --output $(DATASET_DIR) --duration-sec 46 --fps 30 --num-buttons 5 --num-presses 25

demo-local:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli validate --dataset $(DATASET_DIR) --config configs/button_press.yaml --output $(RUN_DIR) --run-id local-demo

scan-local:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli scan --root samples --config configs/button_press.yaml --output-root $(SCAN_OUTPUT_ROOT) --registry

infra-doctor:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli infra doctor

infra-doctor-json:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli infra doctor --output json

# 透传给 `docker build` 的额外参数。默认走阿里云镜像源（见 docker/Dockerfile 中的 ARG）。
# 若要切回官方源（例如海外构建），覆盖如下：
#   make docker-build DOCKER_BUILD_ARGS="\
#     --build-arg PIP_INDEX_URL=https://pypi.org/simple/ \
#     --build-arg TORCH_WHEEL_INDEX=https://download.pytorch.org/whl/cpu \
#     --build-arg TORCH_SPEC=torch"
DOCKER_BUILD_ARGS ?=

docker-build:
	docker build $(DOCKER_BUILD_ARGS) -t robot-data-harness:local -f docker/Dockerfile .

kind-load:
	kind load docker-image robot-data-harness:local --name robot-dh

k8s-apply:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/pvc.yaml
	kubectl apply -f k8s/configmap.yaml
	kubectl apply -f k8s/debug-pod.yaml
	kubectl apply -f k8s/api-deployment.yaml
	kubectl apply -f k8s/api-service.yaml

k8s-status:
	kubectl -n robot-dh get all,pvc

k8s-apply-cronjob:
	kubectl apply -f k8s/scan-cronjob.yaml

k8s-apply-remote-secret:
	@test -f k8s/secret.yaml || (echo "请先根据 k8s/secret.example.yaml 创建 k8s/secret.yaml" && exit 1)
	kubectl apply -f k8s/secret.yaml

k8s-copy-data:
	./scripts/copy_dataset_to_pvc.sh $(DATASET_DIR) button_press_001

k8s-run-job:
	kubectl delete job robot-dh-validator -n robot-dh --ignore-not-found
	kubectl apply -f k8s/validator-job.yaml

k8s-run-job-remote:
	kubectl delete job robot-dh-validator -n robot-dh --ignore-not-found
	kubectl apply -f k8s/validator-job.yaml

k8s-logs:
	kubectl -n robot-dh logs job/robot-dh-validator --all-containers=true

k8s-copy-artifacts:
	./scripts/copy_artifacts_from_pvc.sh button_press_001 $(K8S_ARTIFACT_DIR)

k8s-api-port-forward:
	kubectl -n robot-dh port-forward svc/robot-dh-api 8080:8000

k8s-run-scan-once:
	kubectl -n robot-dh create job $(K8S_SCAN_JOB_NAME) --from=cronjob/robot-dh-scan

k8s-run-scan-remote:
	kubectl -n robot-dh delete job $(K8S_SCAN_JOB_NAME) --ignore-not-found
	kubectl -n robot-dh create job $(K8S_SCAN_JOB_NAME) --from=cronjob/robot-dh-scan

k8s-scan-logs:
	kubectl -n robot-dh logs job/$(K8S_SCAN_JOB_NAME) --all-containers=true

clean-runs:
	rm -rf runs

k8s-clean:
	kubectl delete namespace robot-dh --ignore-not-found

# ---------- v1.4 数据湖 / ETL ----------

lake-doctor:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli infra doctor --check db,s3,redis,lake

lake-list:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli lake list

lake-audit:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli lake audit

normalize-demo-local:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli normalize \
		--dataset $(DATASET_DIR) \
		--output $(LAKE_ODS_LOCAL) \
		--dataset-id $(LAKE_DATASET_ID) \
		--version $(LAKE_DATASET_VERSION) \
		--lake-root $(LAKE_ROOT_LOCAL)

features-demo-local:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli build-features \
		--input $(LAKE_ODS_LOCAL) \
		--output $(LAKE_DWD_LOCAL) \
		--config configs/etl_default.yaml \
		--lake-root $(LAKE_ROOT_LOCAL)

ads-demo-local:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli build-ads \
		--input-root $(LAKE_ROOT_LOCAL)/dwd \
		--output $(LAKE_ADS_LOCAL) \
		--config configs/etl_default.yaml \
		--lake-root $(LAKE_ROOT_LOCAL)

etl-demo-local:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli etl run \
		--dataset $(DATASET_DIR) \
		--dataset-id $(LAKE_DATASET_ID) \
		--version $(LAKE_DATASET_VERSION) \
		--lake-root $(LAKE_ROOT_LOCAL) \
		--build-ads \
		--summary-dir $(LAKE_ROOT_LOCAL)

etl-remote-one:
	@test -n "$$ROBOT_DH_S3_LAKE_BUCKET" || (echo "请先执行: source ~/.config/robot-dh/robot-dh-lake.env" && exit 1)
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli etl run \
		--dataset s3://$(DATASETS_BUCKET_REMOTE)/raw/$(LAKE_DATASET_ID)/$(LAKE_DATASET_VERSION) \
		--dataset-id $(LAKE_DATASET_ID) \
		--version $(LAKE_DATASET_VERSION) \
		--lake-root $(LAKE_ROOT_REMOTE) \
		--build-ads

etl-remote-scan:
	@test -n "$$ROBOT_DH_S3_LAKE_BUCKET" || (echo "请先执行: source ~/.config/robot-dh/robot-dh-lake.env" && exit 1)
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli etl scan \
		--root s3://$(DATASETS_BUCKET_REMOTE) \
		--lake-root $(LAKE_ROOT_REMOTE) \
		--limit 10

k8s-run-etl-remote:
	kubectl -n robot-dh delete job robot-dh-etl --ignore-not-found
	kubectl apply -f k8s/etl-job.yaml

# ---------- v1.4 K8s ETL（k8s/v1_4_lake/）----------

# 名称
LAKE_NS ?= robot-dh
LAKE_SECRET_NAME ?= robot-dh-lake-secrets
LAKE_ETL_ONE_JOB ?= robot-dh-lake-etl-one
LAKE_ETL_SCAN_JOB ?= robot-dh-lake-etl-scan
LAKE_BUILD_ADS_JOB ?= robot-dh-lake-build-ads
LAKE_DEBUG_POD ?= robot-dh-lake-debug
LAKE_CRONJOB ?= robot-dh-lake-etl-scan

k8s-apply-lake-secret-example:
	@echo ""
	@echo "拒绝直接 apply k8s/v1_4_lake/lake-secret.example.yaml。"
	@echo "推荐的安全流程:"
	@echo "  1) source client/robot-dh-lake.env"
	@echo "  2) ./scripts/k8s_create_lake_secret_from_env.sh"
	@echo ""
	@echo "若确需 apply 示例文件（仅适用于可丢弃的 kind 集群，"
	@echo "且占位符 CHANGE_ME 可接受时），请显式执行:"
	@echo "  kubectl apply -f k8s/v1_4_lake/lake-secret.example.yaml"
	@echo ""

k8s-apply-lake:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/v1_4_lake/lake-debug-pod.yaml
	@echo ""
	@echo "说明: lake 相关 Job（etl-one / etl-scan / build-ads）与 CronJob"
	@echo "不会自动 apply，请使用专用 target:"
	@echo "  make k8s-run-etl-one"
	@echo "  make k8s-run-etl-scan"
	@echo "  make k8s-run-build-ads"
	@echo "  make k8s-apply-lake-cron"

k8s-lake-doctor:
	@if kubectl -n $(LAKE_NS) get pod $(LAKE_DEBUG_POD) >/dev/null 2>&1; then \
		echo "[k8s-lake-doctor] 在 $(LAKE_DEBUG_POD) 内执行 robot-dh infra doctor"; \
		kubectl -n $(LAKE_NS) exec $(LAKE_DEBUG_POD) -- robot-dh infra doctor; \
	else \
		echo "未找到 Pod $(LAKE_NS)/$(LAKE_DEBUG_POD)。"; \
		echo "请先执行 make k8s-apply-lake 创建 debug Pod。"; \
		exit 1; \
	fi

k8s-run-etl-one:
	kubectl -n $(LAKE_NS) delete job $(LAKE_ETL_ONE_JOB) --ignore-not-found
	kubectl apply -f k8s/v1_4_lake/lake-etl-one-job.yaml

k8s-run-etl-scan:
	kubectl -n $(LAKE_NS) delete job $(LAKE_ETL_SCAN_JOB) --ignore-not-found
	kubectl apply -f k8s/v1_4_lake/lake-etl-scan-job.yaml

k8s-run-build-ads:
	kubectl -n $(LAKE_NS) delete job $(LAKE_BUILD_ADS_JOB) --ignore-not-found
	kubectl apply -f k8s/v1_4_lake/lake-build-ads-job.yaml

k8s-lake-logs:
	@latest=$$(kubectl -n $(LAKE_NS) get jobs \
		-l component=lake-etl \
		--sort-by=.metadata.creationTimestamp \
		-o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null); \
	if [ -z "$$latest" ]; then \
		echo "命名空间 $(LAKE_NS) 中未找到 lake ETL Job（label component=lake-etl）。"; \
		exit 1; \
	fi; \
	echo "[k8s-lake-logs] 最新 Job: $$latest"; \
	kubectl -n $(LAKE_NS) logs job/$$latest --all-containers=true --tail=500

k8s-apply-lake-cron:
	kubectl apply -f k8s/v1_4_lake/lake-etl-cronjob.yaml

k8s-lake-status:
	kubectl -n $(LAKE_NS) get pods,jobs,cronjobs -l app=robot-dh

k8s-delete-lake-jobs:
	kubectl -n $(LAKE_NS) delete job $(LAKE_ETL_ONE_JOB) --ignore-not-found
	kubectl -n $(LAKE_NS) delete job $(LAKE_ETL_SCAN_JOB) --ignore-not-found
	kubectl -n $(LAKE_NS) delete job $(LAKE_BUILD_ADS_JOB) --ignore-not-found
	@echo "已删除 lake Job。命名空间 $(LAKE_NS)、Secret $(LAKE_SECRET_NAME) 与 debug Pod $(LAKE_DEBUG_POD) 已保留。"

# ---------- v1.5 Scale Benchmark / Sharded ETL ----------

V1_5_BENCH_OUT ?= runs/benchmark/v1_5
V1_5_PLAN ?= runs/plans/scale30_plan.json
V1_5_SUMMARY ?= runs/plans/scale30_summary.json
V1_5_SHARDS_DIR ?= runs/shards/scale30
V1_5_SHARD_ID ?= 0
V1_5_BENCH_SUITE ?= configs/benchmark_suite.yaml
V1_5_SCALE_INCLUDE ?= *scale30*
V1_5_TARGET_SHARD_GB ?= 5
V1_5_MAX_WORKERS ?= 2

benchmark-local: demo-data
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli benchmark run \
		--suite $(V1_5_BENCH_SUITE) \
		--output $(V1_5_BENCH_OUT)

etl-plan-scale30:
	@test -n "$$ROBOT_DH_S3_LAKE_BUCKET" || (echo "请先 source client/robot-dh-v1-5.env" && exit 1)
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli etl plan \
		--root s3://$(DATASETS_BUCKET_REMOTE)/raw \
		--lake-root $(LAKE_ROOT_REMOTE) \
		--include "$(V1_5_SCALE_INCLUDE)" \
		--target-shard-size-gb $(V1_5_TARGET_SHARD_GB) \
		--output $(V1_5_PLAN)

etl-run-shard-0:
	@test -n "$$ROBOT_DH_S3_LAKE_BUCKET" || (echo "请先 source client/robot-dh-v1-5.env" && exit 1)
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli etl run-shard \
		--plan $(V1_5_PLAN) \
		--shard-id $(V1_5_SHARD_ID) \
		--lake-root $(LAKE_ROOT_REMOTE) \
		--output $(V1_5_SHARDS_DIR)/shard_$(V1_5_SHARD_ID) \
		--max-workers $(V1_5_MAX_WORKERS)

etl-merge-scale30:
	PYTHONPATH=src $(PYTHON) -m robot_dh.cli etl merge-summary \
		--plan $(V1_5_PLAN) \
		--shard-results $(V1_5_SHARDS_DIR) \
		--output $(V1_5_SUMMARY)

perf-query:
	@echo "查询 etl_perf_runs / etl_shards / benchmark_runs 需先 source client/robot-dh-v1-5.env"
	@echo "示例: curl http://localhost:8000/etl/perf?dataset_id=button_press_001"
	@echo "      curl http://localhost:8000/etl/shards?plan_id=plan-..."
	@echo "      curl http://localhost:8000/benchmark/runs"
	@echo "      curl http://localhost:8000/events?event_type=etl_shard_finished"

v1-5-smoke:
	$(MAKE) demo-data
	$(MAKE) benchmark-local
	@echo "v1.5 smoke 完成；查看 $(V1_5_BENCH_OUT)/benchmark_report.json"

# ---------- v1.5 Argo Workflows ----------

ARGO_NS ?= argo
ROBOT_DH_NS ?= robot-dh

argo-install:
	./argo/scripts/argo_install.sh

argo-status:
	kubectl get pods -n $(ARGO_NS) -o wide || true
	kubectl -n $(ROBOT_DH_NS) get workflows.argoproj.io,cronworkflows.argoproj.io,workflowtemplates.argoproj.io 2>/dev/null || true

argo-ui-port-forward:
	kubectl -n $(ARGO_NS) port-forward svc/argo-server 2746:2746

k8s-apply-v1-5-secret-example:
	@echo "拒绝直接 apply k8s/v1_5_argo/secret.example.yaml。"
	@echo "推荐安全流程:"
	@echo "  1) source client/robot-dh-v1-5.env"
	@echo "  2) ./scripts/k8s_create_v1_5_secret_from_env.sh"
	@echo ""
	@echo "若坚持 apply 示例文件（仅 kind 调试用）:"
	@echo "  kubectl apply -f k8s/v1_5_argo/secret.example.yaml"

argo-apply-rbac:
	kubectl apply -f k8s/namespace.yaml
	kubectl apply -f k8s/v1_5_argo/serviceaccount.yaml
	kubectl apply -f k8s/v1_5_argo/role.yaml
	kubectl apply -f k8s/v1_5_argo/rolebinding.yaml
	kubectl apply -f k8s/v1_5_argo/configmap.yaml

argo-apply-templates:
	kubectl apply -f argo/templates/

argo-submit-scale-etl:
	./argo/scripts/argo_submit_scale_etl.sh

argo-submit-benchmark:
	./argo/scripts/argo_submit_benchmark.sh

argo-submit-build-ads:
	NS=$(ROBOT_DH_NS) kubectl -n $(ROBOT_DH_NS) create -f argo/workflows/submit-build-ads.yaml

argo-apply-cron:
	kubectl apply -f argo/cron/scale-etl-cronworkflow.yaml

argo-list:
	kubectl -n $(ROBOT_DH_NS) get workflows.argoproj.io --sort-by=.metadata.creationTimestamp

argo-logs:
	./argo/scripts/argo_get_latest_logs.sh

argo-delete-completed:
	./argo/scripts/argo_delete_completed.sh

# ---------- v1.6 Argo log archive（来自 robot-dh-infra 的需求） ----------
# 详见 docs/history/v1_6_argo_log_archive_request.md（接收方文档）和
# docs/history/v1_6_argo_log_archive_handoff.md（完成回执）。

.PHONY: argo-sync-log-archive-secret argo-apply-log-archive \
        argo-verify-log-archive argo-enable-log-archive

# 把 robot-dh/robot-dh-v1-6-secrets 同步到 argo namespace（仅 S3 access/secret key）
argo-sync-log-archive-secret:
	./argo/scripts/argo_sync_log_archive_secret.sh

# 把 archiveLogs + s3 渲染并 patch 进 argo/workflow-controller-configmap，
# 然后 rollout restart deploy/workflow-controller
argo-apply-log-archive:
	./argo/scripts/argo_apply_log_archive.sh

# 验证 ConfigMap + Secret；加 CHECK_OBJECTS=1 还会用 mc 列 argo-logs/
argo-verify-log-archive:
	@if [ "$${CHECK_OBJECTS:-0}" = "1" ]; then \
		./argo/scripts/argo_verify_log_archive.sh --check-objects; \
	else \
		./argo/scripts/argo_verify_log_archive.sh; \
	fi

# 一次跑完三步：sync secret -> apply configmap -> verify
argo-enable-log-archive: argo-sync-log-archive-secret argo-apply-log-archive argo-verify-log-archive

# ---------- v1.5 robot-dh-exporter (Go) ----------

EXPORTER_DIR ?= go/robot-dh-exporter
EXPORTER_IMAGE ?= robot-dh-exporter:local
GOPROXY ?= https://goproxy.cn,direct

exporter-build:
	cd $(EXPORTER_DIR) && GOPROXY=$(GOPROXY) go build -trimpath -ldflags="-s -w" -o ./bin/robot-dh-exporter ./

exporter-test:
	cd $(EXPORTER_DIR) && GOPROXY=$(GOPROXY) go test ./...

exporter-run:
	@test -n "$$ROBOT_DH_DB_URI" || (echo "请先 export ROBOT_DH_DB_URI=postgresql://..." && exit 1)
	cd $(EXPORTER_DIR) && GOPROXY=$(GOPROXY) go run ./

exporter-docker-build:
	cd $(EXPORTER_DIR) && docker build --build-arg GO_PROXY=$(GOPROXY) -t $(EXPORTER_IMAGE) -f Dockerfile .

exporter-kind-load:
	kind load docker-image $(EXPORTER_IMAGE) --name robot-dh

exporter-k8s-apply:
	kubectl apply -f $(EXPORTER_DIR)/k8s/deployment.yaml
	kubectl apply -f $(EXPORTER_DIR)/k8s/service.yaml

exporter-port-forward:
	kubectl -n robot-dh port-forward svc/robot-dh-exporter 9108:9108

exporter-logs:
	kubectl -n robot-dh logs deploy/robot-dh-exporter --tail=200


# ---------- robot platform DAG（多源 Argo Workflow）----------

# 命名空间复用上面已定义的 ROBOT_DH_NS

.PHONY: argo-apply-platform argo-submit-multisource-scale30 argo-submit-contract-qc \
        argo-submit-ml-ready argo-sync-latest argo-platform-logs argo-platform-tail \
        argo-platform-status platform-smoke

argo-apply-platform:
	kubectl apply -f argo/templates/robot-dh-multisource-scale30-workflowtemplate.yaml
	kubectl apply -f argo/templates/robot-dh-contract-qc-workflowtemplate.yaml
	kubectl apply -f argo/templates/robot-dh-ml-ready-workflowtemplate.yaml
	kubectl apply -f argo/cron/multisource-scale30-cronworkflow.yaml

argo-submit-multisource-scale30:
	./scripts/argo_submit_multisource_scale30.sh

argo-submit-contract-qc:
	kubectl -n $(ROBOT_DH_NS) create -f argo/workflows/submit-contract-qc.yaml

argo-submit-ml-ready:
	kubectl -n $(ROBOT_DH_NS) create -f argo/workflows/submit-ml-ready.yaml

argo-sync-latest:
	./scripts/argo_sync_latest.sh

argo-platform-logs:
	@latest=$$(kubectl -n $(ROBOT_DH_NS) get workflows.argoproj.io \
		--sort-by=.metadata.creationTimestamp \
		-o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null); \
	if [ -z "$$latest" ]; then \
		echo "命名空间 $(ROBOT_DH_NS) 中未发现 robot platform Workflow"; \
		exit 1; \
	fi; \
	container="$${LOG_CONTAINER:-main}"; \
	echo "[argo-platform-logs] workflow=$$latest container=$$container (LOG_CONTAINER 覆盖；wait/init 用于排查 executor)"; \
	kubectl -n $(ROBOT_DH_NS) logs -l workflows.argoproj.io/workflow=$$latest \
		-c "$$container" --tail=500 --prefix --max-log-requests=20

argo-platform-tail:
	@wf="$${WF:-}"; \
	if [ -z "$$wf" ]; then \
		wf=$$(kubectl -n $(ROBOT_DH_NS) get workflows.argoproj.io \
			--sort-by=.metadata.creationTimestamp \
			-o jsonpath='{.items[-1:].metadata.name}' 2>/dev/null); \
	fi; \
	if [ -z "$$wf" ]; then \
		echo "命名空间 $(ROBOT_DH_NS) 中未发现 robot platform Workflow"; \
		exit 1; \
	fi; \
	container="$${LOG_CONTAINER:-main}"; \
	echo "[argo-platform-tail] workflow=$$wf container=$$container follow=on"; \
	echo "  注意：kubectl -f 不会自动追入 stream 之后才创建的 step pod，需要 rerun 本目标。"; \
	kubectl -n $(ROBOT_DH_NS) logs -f \
		-l workflows.argoproj.io/workflow=$$wf \
		-c "$$container" --tail=200 --prefix --max-log-requests=20

argo-platform-status:
	kubectl -n $(ROBOT_DH_NS) get workflows.argoproj.io,workflowtemplates.argoproj.io,cronworkflows.argoproj.io

platform-smoke:
	@echo "[platform smoke]"
	@kubectl -n $(ROBOT_DH_NS) get secret robot-dh-v1-6-secrets >/dev/null 2>&1 \
		&& echo "  secret: present" \
		|| echo "  secret: MISSING (run scripts/k8s_create_platform_secret_from_env.sh)"
	@kubectl -n $(ROBOT_DH_NS) get workflowtemplate robot-dh-multisource-scale30 >/dev/null 2>&1 \
		&& echo "  workflowtemplate: present" \
		|| echo "  workflowtemplate: MISSING (run make argo-apply-platform)"
	@docker images robot-data-harness:local --format="  image: {{.Repository}}:{{.Tag}} {{.CreatedSince}}" 2>/dev/null \
		|| echo "  image: MISSING (run make docker-build)"
	@echo "  -> 上述条件齐备后：make argo-submit-multisource-scale30"


# ---------- v1.7 Local-First Data Runtime（Windows D 盘 + 专用 kind）----------
# 详见 docs/history/v1_7_local_data_runtime.md 与 docs/history/v1_7_windows_d_drive_kind_mount.md。
#
# 三层数据策略：
#   devscale  ≤ 3GB    本地 kind robot-dh-dev，默认入口
#   scale30   ~25GiB   远端或夜间压测（沿用 v1.6 make argo-submit-multisource-scale30）
#   full      TB 级     不在本仓库 scope
#
# 关键约束：
#   - 默认 ROBOT_DH_LOCAL_DATA_ROOT=/mnt/d/robot-dh-local，不允许写 C 盘或 WSL VHDX。
#   - 任何破坏性 target（destroy kind / force clean）必须二次确认，不自动执行。
#   - 不自动重建 kind cluster，不自动清理 D 盘数据。

V1_7_LOCAL_DATA_ROOT ?= /mnt/d/robot-dh-local
V1_7_KIND_CLUSTER ?= robot-dh-dev
V1_7_KIND_CONFIG ?= configs/kind-robot-dh-dev-local.yaml
V1_7_DEVSCALE_CONFIG ?= configs/devscale_datasets.yaml

.PHONY: local-preflight local-init-data local-mc-alias local-plan-devscale \
        local-sync-devscale local-verify-devscale local-create-kind-dev \
        local-destroy-kind-dev local-kind-status local-apply-data-pvc \
        local-data-debug local-devscale-summary local-print-dev-env \
        local-runtime-doctor local-datasets-list local-datasets-verify \
        local-adapter-list local-adapter-detect local-adapter-probe \
        local-qc-devscale local-etl-devscale local-ml-ready-devscale \
        local-heartbeat-check local-argo-logs-index v1-7-local-smoke \
        argo-local-apply argo-local-submit argo-local-submit-qc \
        argo-local-submit-ml-ready argo-local-tail argo-local-status \
        argo-local-logs argo-local-sync argo-local-debug \
        argo-local-clean-completed v1-7-local-platform-smoke

local-preflight:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		./scripts/local_preflight_d_drive.sh

local-init-data:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		./scripts/local_init_data_dirs.sh

local-mc-alias:
	@./scripts/local_mc_alias_remote.sh

local-plan-devscale:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		./scripts/local_plan_devscale_sync.sh --config $(V1_7_DEVSCALE_CONFIG)

local-sync-devscale:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		./scripts/local_sync_devscale.sh

local-verify-devscale:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		./scripts/local_verify_devscale.sh

local-create-kind-dev:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		./scripts/local_create_kind_with_d_mount.sh \
			--name $(V1_7_KIND_CLUSTER) \
			--config $(V1_7_KIND_CONFIG)

# 注意：destroy 需要在 stdin 输入 DELETE_DEV_KIND 二次确认，不自动执行。
local-destroy-kind-dev:
	@./scripts/local_destroy_kind_dev.sh --name $(V1_7_KIND_CLUSTER)

local-kind-status:
	@echo "[local-kind-status] cluster=$(V1_7_KIND_CLUSTER)"
	@if kind get clusters 2>/dev/null | grep -qx "$(V1_7_KIND_CLUSTER)"; then \
		echo "  kind cluster: present"; \
		kubectl --context kind-$(V1_7_KIND_CLUSTER) get nodes -o wide 2>/dev/null || true; \
		kubectl --context kind-$(V1_7_KIND_CLUSTER) -n $(ROBOT_DH_NS) get pv,pvc,configmap,pod 2>/dev/null | head -n 30 || true; \
	else \
		echo "  kind cluster: MISSING (run make local-create-kind-dev)"; \
	fi

local-apply-data-pvc:
	kubectl apply -f k8s/v1_7_local/namespace.yaml
	kubectl apply -f k8s/v1_7_local/local-runtime-configmap.yaml
	kubectl apply -f k8s/v1_7_local/local-data-pv-pvc.yaml
	kubectl apply -f k8s/v1_7_local/local-data-debug-pod.yaml
	@echo ""
	@echo "v1.7 本地数据 PV/PVC/ConfigMap/debug-pod 已应用。"
	@echo "进 debug pod 看 raw 数据：make local-data-debug"

local-data-debug:
	@if ! kubectl -n $(ROBOT_DH_NS) get pod robot-dh-local-debug >/dev/null 2>&1; then \
		echo "ERROR: robot-dh-local-debug 不存在，请先 make local-apply-data-pvc。" >&2; \
		exit 1; \
	fi
	@kubectl -n $(ROBOT_DH_NS) wait --for=condition=Ready pod/robot-dh-local-debug --timeout=60s
	@echo "[robot-dh-local-debug] /mnt/local-data/robot-dh-local/raw:"
	@kubectl -n $(ROBOT_DH_NS) exec robot-dh-local-debug -- ls -lh /mnt/local-data/robot-dh-local/raw
	@echo ""
	@echo "[robot-dh-local-debug] /mnt/local-data/robot-dh-local/manifests:"
	@kubectl -n $(ROBOT_DH_NS) exec robot-dh-local-debug -- ls -lh /mnt/local-data/robot-dh-local/manifests 2>/dev/null || true

local-devscale-summary:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		./scripts/local_devscale_summary.sh --config $(V1_7_DEVSCALE_CONFIG)

local-print-dev-env:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		./scripts/local_print_dev_env.sh

# ---------------------------------------------------------------------------
# v1.7 Platform Runtime (Python CLI 接线；不依赖 mc / kind)
# ---------------------------------------------------------------------------

V1_7_RUNTIME_CONFIG ?= configs/devscale_runtime.yaml
V1_7_QC_OUT_DIR ?= $(V1_7_LOCAL_DATA_ROOT)/lake/qc
V1_7_LAKE_ROOT ?= file://$(V1_7_LOCAL_DATA_ROOT)/lake
V1_7_ARGO_ARCHIVE_ROOT ?= s3://robot-dh-artifacts/argo-logs

local-runtime-doctor:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		$(PYTHON) -m robot_dh.cli local runtime doctor \
		--config $(V1_7_RUNTIME_CONFIG) \
		--devscale-config $(V1_7_DEVSCALE_CONFIG)

local-datasets-list:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		$(PYTHON) -m robot_dh.cli local datasets list \
		--config $(V1_7_RUNTIME_CONFIG) \
		--devscale-config $(V1_7_DEVSCALE_CONFIG)

local-datasets-verify:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		$(PYTHON) -m robot_dh.cli local datasets verify \
		--config $(V1_7_RUNTIME_CONFIG) \
		--devscale-config $(V1_7_DEVSCALE_CONFIG)

local-adapter-list:
	@$(PYTHON) -m robot_dh.cli adapter list

# 用法：make local-adapter-detect DATASET_URI=file:///mnt/d/... DATASET_ID=droid_lerobot_dev1g
local-adapter-detect:
	@$(PYTHON) -m robot_dh.cli adapter detect \
		--dataset-uri "$(DATASET_URI)" $(if $(DATASET_ID),--dataset-id $(DATASET_ID),)

local-adapter-probe:
	@$(PYTHON) -m robot_dh.cli adapter probe \
		--dataset-uri "$(DATASET_URI)" $(if $(DATASET_ID),--dataset-id $(DATASET_ID),) \
		$(if $(FAMILY),--family $(FAMILY),)

# 用法：make local-qc-devscale DATASET_ID=droid_lerobot_dev1g
local-qc-devscale:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		$(PYTHON) -m robot_dh.cli qc contract run \
		--dataset-uri file://$(V1_7_LOCAL_DATA_ROOT)/raw/$(DATASET_ID)/v1 \
		--dataset-family $(or $(FAMILY),droid) \
		--out $(V1_7_QC_OUT_DIR)/$(DATASET_ID)/contract_report.json

local-etl-devscale:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		$(PYTHON) -m robot_dh.cli etl run \
		--dataset-id $(DATASET_ID) --version v1 \
		--dataset-uri file://$(V1_7_LOCAL_DATA_ROOT)/raw/$(DATASET_ID)/v1 \
		--lake-root $(V1_7_LAKE_ROOT)

local-ml-ready-devscale:
	@ROBOT_DH_LOCAL_DATA_ROOT=$(V1_7_LOCAL_DATA_ROOT) \
		$(PYTHON) -m robot_dh.cli ml-ready export \
		--dataset-id $(DATASET_ID) --version v1 \
		--lake-root $(V1_7_LAKE_ROOT) \
		--out $(V1_7_LOCAL_DATA_ROOT)/lake/ml-ready/$(DATASET_ID)

local-heartbeat-check:
	@$(PYTHON) -m robot_dh.cli runtime heartbeat check \
		$(if $(WORKFLOW),--workflow-name $(WORKFLOW),) \
		--warn-after-sec $(or $(WARN_SEC),120) \
		--stale-after-sec $(or $(STALE_SEC),300) \
		--fail-on $(or $(FAIL_ON),stale)

local-argo-logs-index:
	@$(PYTHON) -m robot_dh.cli argo logs index \
		--workflow-name $(WORKFLOW) \
		--namespace $(or $(NS),robot-dh) \
		--archive-root $(V1_7_ARGO_ARCHIVE_ROOT) \
		$(if $(DRY_RUN),--dry-run,)

# 一键 dry-run：runtime doctor + datasets list + adapter list + heartbeat check（无远端依赖）
v1-7-local-smoke:
	@echo "== v1.7 platform smoke =="
	@$(PYTHON) -m robot_dh.cli adapter list >/dev/null && echo "[ok] adapter list"
	@$(PYTHON) -m robot_dh.cli runtime heartbeat check \
		--events-dir /tmp/robot-dh-smoke-events \
		--warn-after-sec 60 --stale-after-sec 300 --fail-on never \
		>/dev/null && echo "[ok] heartbeat check (no events)"
	@$(PYTHON) -m robot_dh.cli local datasets list \
		--config $(V1_7_RUNTIME_CONFIG) \
		--devscale-config $(V1_7_DEVSCALE_CONFIG) \
		>/dev/null && echo "[ok] devscale datasets list"
	@echo "== smoke passed =="

# ---------------------------------------------------------------------------
# v1.7 Argo Local WorkflowTemplate（仅在 kind-robot-dh-dev context 下提交）
# ---------------------------------------------------------------------------

V1_7_ARGO_DIR ?= argo/v1_7_local
V1_7_ARGO_NS ?= robot-dh

argo-local-apply:
	@echo "== apply v1.7 Local Argo RBAC / ConfigMap / WorkflowTemplate / CronWorkflow =="
	@# v1.7 local-argo-rbac.yaml 只挂额外 Role/RoleBinding，复用 v1.5 的 ServiceAccount + 基础 Role；
	@# 新建 kind 集群时必须先 apply v1.5 这三件套，否则 step pod 在 admission 阶段被拒（SA not found）。
	kubectl apply -f k8s/v1_5_argo/serviceaccount.yaml
	kubectl apply -f k8s/v1_5_argo/role.yaml
	kubectl apply -f k8s/v1_5_argo/rolebinding.yaml
	kubectl apply -f k8s/v1_7_local/local-argo-rbac.yaml
	kubectl apply -f k8s/v1_7_local/local-argo-configmap.yaml
	kubectl apply -f k8s/v1_7_local/local-runtime-configmap.yaml
	kubectl apply -f $(V1_7_ARGO_DIR)/templates/
	kubectl apply -f $(V1_7_ARGO_DIR)/cron/local-devscale-cronworkflow.yaml
	@echo "[ok] v1.7 Argo Local resources applied. 提交：make argo-local-submit"

argo-local-submit:
	@./$(V1_7_ARGO_DIR)/scripts/submit_local_devscale.sh

argo-local-submit-qc:
	@kubectl -n $(V1_7_ARGO_NS) create -f $(V1_7_ARGO_DIR)/workflows/submit-local-qc.yaml

argo-local-submit-ml-ready:
	@kubectl -n $(V1_7_ARGO_NS) create -f $(V1_7_ARGO_DIR)/workflows/submit-local-ml-ready.yaml

# 用法：make argo-local-tail [WF=robot-dh-local-devscale-xxxxx]
argo-local-tail:
	@WF="$${WF:-$$(kubectl -n $(V1_7_ARGO_NS) get wf -l role=devscale-main -o jsonpath='{.items[-1:].metadata.name}')}"; \
		if [ -z "$$WF" ]; then echo "no devscale workflow found, run: make argo-local-submit" >&2; exit 1; fi; \
		echo "tailing $$WF"; \
		./$(V1_7_ARGO_DIR)/scripts/tail_live_workflow_logs.sh "$$WF" --ns $(V1_7_ARGO_NS)

argo-local-status:
	@kubectl -n $(V1_7_ARGO_NS) get wf -l role=devscale-main -o wide || true
	@echo ""
	@kubectl -n $(V1_7_ARGO_NS) get wf -l role=devscale-qc -o wide 2>/dev/null || true
	@kubectl -n $(V1_7_ARGO_NS) get wf -l role=devscale-ml-ready -o wide 2>/dev/null || true
	@kubectl -n $(V1_7_ARGO_NS) get cronworkflow.argoproj.io -l role=devscale-main-cron 2>/dev/null || true

argo-local-logs:
	@WF="$${WF:-$$(kubectl -n $(V1_7_ARGO_NS) get wf -l role=devscale-main -o jsonpath='{.items[-1:].metadata.name}')}"; \
		if [ -z "$$WF" ]; then echo "no devscale workflow found" >&2; exit 1; fi; \
		echo "==== last devscale workflow: $$WF ===="; \
		kubectl -n $(V1_7_ARGO_NS) logs -l workflows.argoproj.io/workflow=$$WF -c main --tail=500 --prefix --max-log-requests=20

# argo sync + archive logs index 写回 PG（dry-run 可用）
# 用法：make argo-local-sync [WF=...] [DRY_RUN=1]
argo-local-sync:
	@WF="$${WF:-$$(kubectl -n $(V1_7_ARGO_NS) get wf -l role=devscale-main -o jsonpath='{.items[-1:].metadata.name}')}"; \
		if [ -z "$$WF" ]; then echo "no devscale workflow found" >&2; exit 1; fi; \
		args=""; [ -n "$$DRY_RUN" ] && args="--dry-run"; \
		./$(V1_7_ARGO_DIR)/scripts/sync_workflow_steps.sh "$$WF" --ns $(V1_7_ARGO_NS) $$args

argo-local-debug:
	@kubectl -n $(V1_7_ARGO_NS) apply -f k8s/v1_7_local/local-data-debug-pod.yaml
	@kubectl -n $(V1_7_ARGO_NS) wait --for=condition=Ready pod/robot-dh-local-debug --timeout=60s
	@echo "[robot-dh-local-debug] /mnt/local-data/robot-dh-local/raw:"
	@kubectl -n $(V1_7_ARGO_NS) exec robot-dh-local-debug -- ls -lh /mnt/local-data/robot-dh-local/raw

# 清理 7 天前已完成（Succeeded/Failed/Error）的 devscale workflow
argo-local-clean-completed:
	@kubectl -n $(V1_7_ARGO_NS) get wf -l component=v1-7-local \
		-o jsonpath='{range .items[?(@.status.phase=="Succeeded")]}{.metadata.name}{"\n"}{end}{range .items[?(@.status.phase=="Failed")]}{.metadata.name}{"\n"}{end}{range .items[?(@.status.phase=="Error")]}{.metadata.name}{"\n"}{end}' \
		| while read wf; do [ -n "$$wf" ] && kubectl -n $(V1_7_ARGO_NS) delete wf "$$wf"; done; true

# v1.7 Local Platform smoke：本地 doctor + verify + image + kind context + pvc + template，
# **不**真的提交 workflow，给用户提示下一步命令。
v1-7-local-platform-smoke:
	@echo "== v1.7 local PLATFORM smoke =="
	@echo "[1/6] robot-dh local runtime doctor (no allow-over-limit)"
	@$(PYTHON) -m robot_dh.cli local runtime doctor \
		--config $(V1_7_RUNTIME_CONFIG) \
		--devscale-config $(V1_7_DEVSCALE_CONFIG) \
		--allow-over-limit >/dev/null && echo "    [ok] runtime doctor" || echo "    [warn] runtime doctor reports issues; 见上面 JSON"
	@echo "[2/6] robot-dh local datasets list"
	@$(PYTHON) -m robot_dh.cli local datasets list \
		--config $(V1_7_RUNTIME_CONFIG) \
		--devscale-config $(V1_7_DEVSCALE_CONFIG) \
		>/dev/null && echo "    [ok] devscale datasets list"
	@echo "[3/6] docker image robot-data-harness:local"
	@if docker images --format '{{.Repository}}:{{.Tag}}' 2>/dev/null | grep -q '^robot-data-harness:local$$'; then \
		echo "    [ok] image present"; \
	else echo "    [warn] robot-data-harness:local 不存在，先 make docker-build && make kind-load"; fi
	@echo "[4/6] kubectl context"
	@ctx=$$(kubectl config current-context 2>/dev/null || echo ""); \
		echo "    current=$$ctx (期望 kind-robot-dh-dev)"; \
		if [ "$$ctx" = "kind-robot-dh-dev" ]; then echo "    [ok]"; else echo "    [warn] 不是 kind-robot-dh-dev，提交前请：kubectl config use-context kind-robot-dh-dev"; fi
	@echo "[5/6] PVC robot-dh-local-data-pvc"
	@if kubectl -n $(V1_7_ARGO_NS) get pvc robot-dh-local-data-pvc >/dev/null 2>&1; then \
		echo "    [ok] PVC exists"; \
	else echo "    [warn] PVC 不存在，先 make local-apply-data-pvc"; fi
	@echo "[6/6] WorkflowTemplate robot-dh-local-devscale"
	@if kubectl -n $(V1_7_ARGO_NS) get workflowtemplate robot-dh-local-devscale >/dev/null 2>&1; then \
		echo "    [ok] WorkflowTemplate exists"; \
	else echo "    [warn] 模板不存在，先 make argo-local-apply"; fi
	@echo ""
	@echo "== v1.7 local PLATFORM smoke completed =="
	@echo "下一步：make argo-local-submit  # 然后 make argo-local-tail 看实时日志"

# ---------------------------------------------------------------------------
# v1.8 Warehouse Metrics & Quality Ops
# ---------------------------------------------------------------------------
#
# 设计要点：
#   - 本节命令默认走 ROBOT_DH_DB_URI（远端 PostgreSQL）；
#     未配置时 fallback 到 ./.robot_dh/robot_dh.db（SQLite），适合 make test / 离线 demo。
#   - warehouse build 在 SQLite 走 Python 端聚合；PostgreSQL 走 warehouse/sql/dml/*.sql。
#   - quality / sla / backfill 三个 target 不依赖远端 MinIO / Redis，可在 WSL 单机跑通。
#
# 关键变量（可命令行覆盖）：
#   V1_8_DATE                  默认昨天（UTC）
#   V1_8_FROM / V1_8_TO        默认 V1_8_DATE
#   V1_8_LAYERS                逗号分隔，默认 'dim,fact,dws,ads'
#   V1_8_WAREHOUSE_CONFIG      默认 configs/warehouse.yaml
#   V1_8_SLA_POLICY            默认 configs/sla_policies.yaml
#   V1_8_QUERY_TABLE           warehouse query 表名
#   V1_8_REPORT_OUT            quality report / sla report / backfill plan 输出目录

V1_8_DATE ?= $(shell date -u -d 'yesterday' +%Y-%m-%d 2>/dev/null || python3 -c "from datetime import datetime,timedelta; print((datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%d'))")
V1_8_FROM ?= $(V1_8_DATE)
V1_8_TO ?= $(V1_8_DATE)
V1_8_LAYERS ?= dim,fact,dws,ads
V1_8_WAREHOUSE_CONFIG ?= configs/warehouse.yaml
V1_8_SLA_POLICY ?= configs/sla_policies.yaml
V1_8_QUERY_TABLE ?= ads_quality_dashboard
V1_8_REPORT_OUT ?= runs/v1_8

.PHONY: warehouse-init warehouse-build-local warehouse-query warehouse-export-local \
        quality-summary quality-report backfill-plan sla-check v1-8-warehouse-smoke \
        spark-install spark-build-quality-ads-local spark-test

warehouse-init:
	@$(PYTHON) -m robot_dh.cli warehouse init --config $(V1_8_WAREHOUSE_CONFIG) $(if $(APPLY_DDL),--apply-ddl,)

warehouse-build-local:
	@mkdir -p $(V1_8_REPORT_OUT)
	@$(PYTHON) -m robot_dh.cli warehouse build \
		--config $(V1_8_WAREHOUSE_CONFIG) \
		--from-date $(V1_8_FROM) --to-date $(V1_8_TO) \
		--layers $(V1_8_LAYERS) \
		--output-root file://$(abspath $(V1_8_REPORT_OUT))/build

warehouse-query:
	@$(PYTHON) -m robot_dh.cli warehouse query \
		--config $(V1_8_WAREHOUSE_CONFIG) \
		--table $(V1_8_QUERY_TABLE) \
		--limit $(or $(LIMIT),20) \
		--output $(or $(FORMAT),table)

warehouse-export-local:
	@mkdir -p $(V1_8_REPORT_OUT)/export
	@$(PYTHON) -m robot_dh.cli warehouse export \
		--config $(V1_8_WAREHOUSE_CONFIG) \
		--table $(V1_8_QUERY_TABLE) \
		--date $(V1_8_DATE) \
		--format $(or $(FORMAT),parquet) \
		--output file://$(abspath $(V1_8_REPORT_OUT))/export/$(V1_8_QUERY_TABLE)/dt=$(V1_8_DATE)

quality-summary:
	@$(PYTHON) -m robot_dh.cli quality summary --date $(V1_8_DATE) --output json

quality-report:
	@mkdir -p $(V1_8_REPORT_OUT)/quality_report
	@$(PYTHON) -m robot_dh.cli quality report \
		--date $(V1_8_DATE) \
		--output $(V1_8_REPORT_OUT)/quality_report

backfill-plan:
	@mkdir -p $(V1_8_REPORT_OUT)/backfill
	@$(PYTHON) -m robot_dh.cli backfill plan \
		--from-date $(V1_8_FROM) --to-date $(V1_8_TO) \
		$(if $(DATASET_ID),--dataset $(DATASET_ID),) \
		$(if $(VERSION),--version $(VERSION),) \
		$(if $(PHASE),--phase $(PHASE),) \
		$(if $(REASON),--reason "$(REASON)",) \
		$(if $(DRY_RUN),--dry-run,) \
		--output $(V1_8_REPORT_OUT)/backfill

sla-check:
	@$(PYTHON) -m robot_dh.cli sla check --date $(V1_8_DATE) --policy $(V1_8_SLA_POLICY) \
		$(if $(DRY_RUN),--dry-run,)

# v1.8 smoke：本地一次性回环（不依赖远端，SQLite 也能跑）
v1-8-warehouse-smoke:
	@echo "== v1.8 warehouse smoke =="
	@$(MAKE) warehouse-init >/dev/null 2>&1 || true
	@echo "[1/6] warehouse init"
	@$(PYTHON) -m robot_dh.cli warehouse init --config $(V1_8_WAREHOUSE_CONFIG) | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    backend=%s  schema=%s  existing=%d  missing=%d' % (d['backend'], d['schema'], len(d['existing_tables']), len(d['missing_tables'])))"
	@echo "[2/6] warehouse build $(V1_8_DATE)"
	@$(PYTHON) -m robot_dh.cli warehouse build --config $(V1_8_WAREHOUSE_CONFIG) --date $(V1_8_DATE) | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    status=%s  layers=%s' % (d['status'], d['layers']))"
	@echo "[3/6] warehouse query ads_quality_dashboard"
	@$(PYTHON) -m robot_dh.cli warehouse query --config $(V1_8_WAREHOUSE_CONFIG) --table ads_quality_dashboard --limit 5 --output table | head -5 | sed 's/^/    /'
	@echo "[4/6] quality report -> $(V1_8_REPORT_OUT)/quality_report"
	@mkdir -p $(V1_8_REPORT_OUT)/quality_report
	@$(PYTHON) -m robot_dh.cli quality report --date $(V1_8_DATE) --output $(V1_8_REPORT_OUT)/quality_report | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    summary_html=%s' % d['summary_html'])"
	@echo "[5/6] backfill plan dry-run"
	@$(PYTHON) -m robot_dh.cli backfill plan --from-date $(V1_8_FROM) --to-date $(V1_8_TO) --dry-run | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    plan_id=%s  task_count=%d  dry_run=%s' % (d['plan_id'], d['task_count'], d['dry_run']))"
	@echo "[6/6] sla check"
	@$(PYTHON) -m robot_dh.cli sla check --date $(V1_8_DATE) --policy $(V1_8_SLA_POLICY) --dry-run | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); checks=d.get('checks',[]); print('    checks=%d' % len(checks))" || true
	@echo "== v1.8 warehouse smoke completed =="

# v1.8 promptC：Spark local mode 可选离线宽表
# 用法：
#   make spark-install                                              # 装 pyspark optional extra
#   make spark-build-quality-ads-local                              # 跑 SparkSQL local mode
#       SPARK_INPUT=...   warehouse export 根目录，file:// 或本地路径（必填）
#       SPARK_OUTPUT=...  parquet 输出根目录（必填）
#       SPARK_DATE=YYYY-MM-DD  默认 V1_8_DATE
#   make spark-test                                                 # 跑 spark optional 测试

SPARK_INPUT  ?= file://$(abspath $(V1_8_REPORT_OUT))/export
SPARK_OUTPUT ?= file://$(abspath $(V1_8_REPORT_OUT))/spark_ads
SPARK_DATE   ?= $(V1_8_DATE)

spark-install:
	@$(PIP) install -e ".[spark]"

spark-build-quality-ads-local:
	@$(PYTHON) -m robot_dh.cli spark build-quality-ads \
		--input "$(SPARK_INPUT)" \
		--output "$(SPARK_OUTPUT)" \
		--date $(SPARK_DATE)

spark-test:
	@$(PYTEST) tests/test_spark_quality_ads_optional.py -q

# ============================================================
# v1.9 AI Inference Data Plane Lite（本地，不依赖 GPU / 远端）
# ============================================================
.PHONY: model-init model-list infer-mock-local infer-benchmark-local distill-build-local v1-9-inference-smoke

V1_9_RUN        ?= runs/v1_9_smoke
V1_9_INPUT      ?= file://$(abspath $(V1_9_RUN))/ml-ready/demo/v1
V1_9_INFER_OUT  ?= file://$(abspath $(V1_9_RUN))/infer/captions/demo/v1
V1_9_DISTILL_OUT?= file://$(abspath $(V1_9_RUN))/distill/demo/v1
V1_9_BENCH_OUT  ?= file://$(abspath $(V1_9_RUN))/benchmark/mock-captioner
V1_9_DATE       ?= $(shell date -u +%F)
V1_9_MODEL      ?= mock-captioner-v1

model-init:
	@$(PYTHON) -m robot_dh.cli model register --config configs/model_registry.yaml

model-list:
	@$(PYTHON) -m robot_dh.cli model list

# 生成一个最小 ML-ready parquet 供本地推理使用。
$(V1_9_RUN)/ml-ready/demo/v1/train.parquet:
	@mkdir -p $(V1_9_RUN)/ml-ready/demo/v1
	@$(PYTHON) -c "import pyarrow as pa, pyarrow.parquet as pq; pq.write_table(pa.table({'episode_id':[f'e{i}' for i in range(20)],'quality_score':[0.5+0.02*i for i in range(20)]}), '$(V1_9_RUN)/ml-ready/demo/v1/train.parquet')"

infer-mock-local: $(V1_9_RUN)/ml-ready/demo/v1/train.parquet model-init
	@$(PYTHON) -m robot_dh.cli infer run --input "$(V1_9_INPUT)" --model-id $(V1_9_MODEL) \
		--output "$(V1_9_INFER_OUT)" --batch-size 8 --max-workers 4

infer-benchmark-local: $(V1_9_RUN)/ml-ready/demo/v1/train.parquet model-init
	@$(PYTHON) -m robot_dh.cli infer benchmark --input "$(V1_9_INPUT)" --model-id $(V1_9_MODEL) \
		--output "$(V1_9_BENCH_OUT)" --concurrency 1,2,4 --batch-size 8,16 --limit 20

distill-build-local: infer-mock-local
	@$(PYTHON) -m robot_dh.cli distill build --teacher-output "$(V1_9_INFER_OUT)" \
		--format instruction_tuning --output "$(V1_9_DISTILL_OUT)" --split 0.8,0.1,0.1

v1-9-inference-smoke: $(V1_9_RUN)/ml-ready/demo/v1/train.parquet
	@echo "== v1.9 inference smoke =="
	@echo "[1/7] model register"
	@$(PYTHON) -m robot_dh.cli model register --config configs/model_registry.yaml | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    registered=%d backend=%s' % (len(d['registered']), d['backend']))"
	@echo "[2/7] model list"
	@$(PYTHON) -m robot_dh.cli model list | python3 -c "import sys,json; print('    models=%d' % len(json.loads(sys.stdin.read())))"
	@echo "[3/7] infer run $(V1_9_MODEL)"
	@$(PYTHON) -m robot_dh.cli infer run --input "$(V1_9_INPUT)" --model-id $(V1_9_MODEL) --output "$(V1_9_INFER_OUT)" --batch-size 8 --max-workers 4 | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    status=%s total=%s sps=%s' % (d['status'], d['report']['total_samples'], d['report']['samples_per_sec']))"
	@echo "[4/7] infer benchmark"
	@$(PYTHON) -m robot_dh.cli infer benchmark --input "$(V1_9_INPUT)" --model-id $(V1_9_MODEL) --output "$(V1_9_BENCH_OUT)" --concurrency 1,2 --batch-size 8,16 --limit 20 | python3 -c "import sys,json; print('    combos=%d' % json.loads(sys.stdin.read())['combo_count'])"
	@echo "[5/7] distill build"
	@$(PYTHON) -m robot_dh.cli distill build --teacher-output "$(V1_9_INFER_OUT)" --format instruction_tuning --output "$(V1_9_DISTILL_OUT)" --split 0.8,0.1,0.1 | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    num_total=%s train=%s' % (d['num_total'], d['num_train']))"
	@echo "[6/7] warehouse build inference layer"
	@$(PYTHON) -m robot_dh.cli warehouse build --layers inference --date $(V1_9_DATE) | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    status=%s layers=%s' % (d['status'], d['layers']))"
	@echo "[7/7] quality summary (inference metrics)"
	@$(PYTHON) -m robot_dh.cli quality summary --date $(V1_9_DATE) | python3 -c "import sys,json; d=json.loads(sys.stdin.read()); print('    inference_job_count=%s inference_success_rate=%s' % (d.get('inference_job_count'), d.get('inference_success_rate')))"
	@echo "== v1.9 inference smoke completed =="
