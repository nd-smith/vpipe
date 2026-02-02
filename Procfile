web: cd src && if [ "$WORKER_TYPE" = "plugin" ]; then \
  case "$WORKER_NAME" in \
    "itel-cabinet-tracking") python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_tracking_worker ;; \
    "itel-cabinet-api") python -m pipeline.plugins.itel_cabinet_api.itel_cabinet_api_worker ;; \
    "claimx-mitigation-tracking") python -m pipeline.plugins.claimx_mitigation_task.mitigation_tracking_worker ;; \
    *) echo "Unknown plugin worker: $WORKER_NAME" && exit 1 ;; \
  esac; \
else \
  python -m pipeline --worker ${WORKER_NAME:-xact-poller} --metrics-port ${PORT:-8080}; \
fi
