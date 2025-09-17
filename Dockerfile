FROM ghcr.io/prefix-dev/pixi:latest

ENV TZ="America/New York"

RUN apt-get -y update && \
    apt-get -y install git

COPY pixi.toml .
COPY pixi.lock .
# use `--locked` to ensure the lockfile is up to date with pixi.toml
RUN pixi install --locked
# create the shell-hook bash script to activate the environment
RUN pixi shell-hook -s bash > /shell-hook

ENV PYTHONUNBUFFERED=1
# either here or when docker run is done, make sure that the TILED_SITE_PROFILES directory on disk is mounted to the container. this will be necessary for data access. we will also need to allow outside network access from the container to get data from tiled

COPY test.py .

ENV TILED_API_KEY=""
RUN mkdir /etc/tiled
RUN mkdir /.prefect -m 0777
RUN mkdir /repo -m 0777
RUN /bin/bash /shell-hook

#now reapply deployment to push the image that is being created
ENTRYPOINT ["pixi", "run"]
CMD ["python", "-m", "test", "arg"]
