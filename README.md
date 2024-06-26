## Airflow 사용 방법

### 필요 설정
```
docker, docker-compose 다운 필요
```

### airflow 실행하기
```
- 실행하기
docker-compose.yaml 파일이 있는 폴더에 들어가서
docker-compose up (-d)

- 종료하기
(volume을 업데이트 했다면 -v | volume은 폴더 위치 설정)
docker-compose down (-v)
```

### airflow webserver 접속
```
현재 localhost:8080 으로 접속할 수 있음
ID : airflow
pwd : airflow
```

### dag 실행
```
dags 실행하기 위해서 secret 키 넣어야 함
```
