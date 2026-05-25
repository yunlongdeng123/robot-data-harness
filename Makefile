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
        exporter-k8s-apply exporter-port-forward exporter-logs

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
# 详见 docs/v1_6_argo_log_archive_request.md（接收方文档）和
# docs/v1_6_argo_log_archive_handoff.md（完成回执）。

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
