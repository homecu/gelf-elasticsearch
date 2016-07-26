FROM jmayfield/shellish
RUN pip install aiohttp
COPY . src
WORKDIR src
ENTRYPOINT python ./gelf_elasticsearch.py
