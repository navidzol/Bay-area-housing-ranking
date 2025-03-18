#!/bin/bash
if [ "$1" == "" ]; then
  echo "Usage: ./view-logs.sh [processor|api|db|frontend|all]"
  exit 1
fi

case "$1" in
  processor)
    docker logs -f bay_area_housing_processor
    ;;
  api)
    docker logs -f bay_area_housing_api
    ;;
  db)
    docker logs -f postgis_db
    ;;
  frontend)
    docker logs -f bay_area_housing_frontend
    ;;
  all)
    docker compose logs -f
    ;;
  *)
    echo "Unknown service: $1"
    echo "Usage: ./view-logs.sh [processor|api|db|frontend|all]"
    exit 1
    ;;
esac
