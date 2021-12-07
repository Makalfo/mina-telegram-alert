FROM ubuntu:20.04

# update and install packages
RUN apt-get update \
  && apt-get update \
  && apt-get install python3 python3-pip git openssh-client apt-utils -y

# clone the repo and install the requirements
RUN git clone https://github.com/Makalfo/mina-telegram-alert.git
WORKDIR /mina-telegram-alert/
RUN pip3 install -r requirements.txt

# run the script
CMD python3 mina-telegram-alert.py