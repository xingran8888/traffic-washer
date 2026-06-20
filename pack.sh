# traffic-washer v5.6 - 流量清洗工具
# 打包文件
tar -czf traffic-washer-v5.6.tar.gz \
    app.py \
    url_pool.py \
    Dockerfile \
    docker-compose.yml \
    requirements.txt \
    README.md \
    .gitignore

echo "tar created:"
ls -lh traffic-washer-v5.6.tar.gz
