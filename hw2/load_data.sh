set -e

docker exec master mkdir -p /tmp/movielens_data
docker cp ./data/. master:/tmp/movielens_data/

docker exec master bash -c "

    load_to_hdfs() {
        target_dir=/user/hive/warehouse/movielens/\$1
        file_name=\$2
        
        echo \" Обработка \$file_name...\"
        hdfs dfs -mkdir -p \$target_dir
        hdfs dfs -put -f /tmp/movielens_data/\$file_name \$target_dir/
    }

    load_to_hdfs 'movies'        'movie.csv'
    load_to_hdfs 'ratings'       'rating.csv'
    load_to_hdfs 'tags'          'tag.csv'
    load_to_hdfs 'links'         'link.csv'
    load_to_hdfs 'genome_scores' 'genome_scores.csv'
    load_to_hdfs 'genome_tags'   'genome_tags.csv'
    
    rm -rf /tmp/movielens_data
echo \"Файлы загружены успешно\"
"

