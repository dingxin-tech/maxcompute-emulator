FROM python:3.11-slim-bookworm
RUN sed -i 's|http://deb.debian.org|https://deb.debian.org|g' /etc/apt/sources.list.d/debian.sources && apt-get -o Acquire::https::Timeout=30 update && apt-get install -y --no-install-recommends g++ libboost-dev libprotobuf-dev git && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir pyarrow==19.0.1
RUN git clone https://github.com/aliyun/aliyun-odps-sdk-cpp.git /sdk && git -C /sdk checkout a1a7e541753f53970131e345a4f77581a8d6a9ac
COPY reader.cpp /probe/reader.cpp
RUN python -c 'import pyarrow; pyarrow.create_library_symlinks()' && g++ -std=c++17 -DODPS_SDK_ENABLE_ARROW -I/sdk -I/sdk/tunnel -I/usr/local/lib/python3.11/site-packages/pyarrow/include /probe/reader.cpp -L/usr/local/lib/python3.11/site-packages/pyarrow -Wl,-rpath,/usr/local/lib/python3.11/site-packages/pyarrow -larrow -o /probe/reader
COPY run.py /probe/run.py
ENTRYPOINT ["python", "/probe/run.py"]
