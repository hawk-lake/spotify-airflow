#syntax=docker/dockerfile:1
FROM ubuntu:latest

# requirements 파일을 이미지 내부로 이동
COPY apt_requirements.txt /apt_requirements.txt
COPY pip_requirements.txt /pip_requirements.txt
RUN ["apt-get", "update"]

# requirements 설치
RUN ["xargs", "-a", "/apt_requirements.txt", "apt-get", "install", "-y"]
RUN ["pip3", "install", "-r", "/pip_requirements.txt"]

# <추가>
# git 레포지토리 복사
# RUN ["git", "clone", "-b", "<branch_name>", "<git_link>", "/<container_directory>"]

# <추가>
# 모니터링용 node-exporter 설치
RUN ["wget", "https://github.com/prometheus/node_exporter/releases/download/v1.6.1/node_exporter-1.6.1.linux-amd64.tar.gz"]
RUN ["tar", "xvf", "node_exporter-1.6.1.linux-amd64.tar.gz"]
RUN ["cp", "node_exporter-1.6.1.linux-amd64/node_exporter", "/usr/local/bin/"]

# 설치 및 구성한 application 실행
WORKDIR /
CMD ["bash", "-c", "service cron start & node_exporter"]