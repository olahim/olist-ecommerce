#!/bin/bash
# Airflow control script

ACTION=$1

case $ACTION in
    start)
        echo "Starting Airflow services..."
        airflow scheduler -D
        airflow webserver -D
        echo "Airflow started. UI: http://localhost:8080"
        ;;
    stop)
        echo "Stopping Airflow services..."
        pkill -f "airflow scheduler" || true
        pkill -f "airflow webserver" || true
        echo "Airflow stopped"
        ;;
    restart)
        $0 stop
        sleep 2
        $0 start
        ;;
    status)
        if pgrep -f "airflow scheduler" > /dev/null; then
            echo "Scheduler: RUNNING"
        else
            echo "Scheduler: STOPPED"
        fi
        if pgrep -f "airflow webserver" > /dev/null; then
            echo "Webserver: RUNNING"
        else
            echo "Webserver: STOPPED"
        fi
        ;;
    trigger)
        DAG=$2
        if [ -z "$DAG" ]; then
            echo "Usage: $0 trigger <dag_name>"
            exit 1
        fi
        airflow dags trigger "$DAG"
        ;;
    unpause)
        DAG=$2
        if [ -z "$DAG" ]; then
            echo "Usage: $0 unpause <dag_name>"
            exit 1
        fi
        airflow dags unpause "$DAG"
        ;;
    pause)
        DAG=$2
        if [ -z "$DAG" ]; then
            echo "Usage: $0 pause <dag_name>"
            exit 1
        fi
        airflow dags pause "$DAG"
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|trigger|unpause|pause} [dag_name]"
        exit 1
        ;;
esac