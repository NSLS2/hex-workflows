FROM ghcr.io/prefix-dev/pixi:0.57.0

ENV TZ="America/New_York"

RUN apt-get -y update && \
    apt-get -y install git tzdata

COPY pixi.toml .
COPY pixi.lock .
# use `--locked` to ensure the lockfile is up to date with pixi.toml
RUN pixi install --locked
# create the shell-hook bash script to activate the environment
RUN pixi shell-hook -s bash > /shell-hook

ENV PYTHONUNBUFFERED=1

COPY default.py .

RUN mkdir /etc/tiled
RUN mkdir /.prefect -m 0777
RUN mkdir /repo -m 0777

RUN /bin/bash /shell-hook

#now reapply deployment to push the image that is being created
ENTRYPOINT ["pixi", "run"]
CMD ["python", "-m", "default", "arg"]
