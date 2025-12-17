Последовательность команд
```
docker compose up
docker cp shadow.txt hadoop-client:/tmp/shadow.txt
docker cp run_tasks.sh hadoop-client:/run_tasks.sh
docker exec -it hadoop-client bash -c "bash /run_tasks.sh" >> results.log
```
Результат выполнения команд находится в result.log
