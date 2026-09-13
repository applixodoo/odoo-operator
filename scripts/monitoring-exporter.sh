#!/bin/sh
# Keep the container Ready while the best-effort exporter child restarts.
# A whole-container OOM still affects Pod readiness; validate limits in canary.
child=
stop() {
    trap '' TERM INT
    if [ -n "$child" ]; then
        kill -TERM "$child" 2>/dev/null || true
        wait "$child" 2>/dev/null || true
    fi
    exit 0
}
trap stop TERM INT
while :; do
    (
        # statsd_exporter refuses a stale socket after an unclean exit.
        rm -f /run/droggol-monitoring/statsd.sock && exec /bin/statsd_exporter "$@"
    ) &
    child=$!
    wait "$child" || true
    child=
    sleep 2 &
    child=$!
    wait "$child" || true
    child=
done
