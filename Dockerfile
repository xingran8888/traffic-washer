FROM python:3.10-alpine
WORKDIR /app
RUN pip install flask requests -i https://pypi.tuna.tsinghua.edu.cn/simple --no-cache-dir
COPY app.py .
EXPOSE 9999
CMD ["python3", "app.py"]