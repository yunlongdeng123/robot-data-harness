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
        k8s-lake-logs k8s-apply-lake-cron k8s-lake-status k8s-delete-lake-jobs

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
