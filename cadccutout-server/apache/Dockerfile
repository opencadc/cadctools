FROM centos:8

ENV PYTHON_MAJOR_VERSION="3"

RUN yum clean all expire-cache
RUN yum makecache
# https://centos7.iuscommunity.org/ius-release.rpm is the CentOS community repo. httpd24u is the updated version of apache
# built there, and replaces the base httpd package
RUN yum -y --setopt=tsflags=nodocs install -y epel-release && \
    yum -y --setopt=tsflags=nodocs update && \
    yum -y --setopt=tsflags=nodocs install httpd coreutils-single findutils sed patch
    
RUN yum -y --setopt=tsflags=nodocs install autoconf bzip2-devel gcc gcc-c++ git libffi-devel make openssl-devel perl python3 python3-devel

RUN pip${PYTHON_MAJOR_VERSION} install \
   -e 'git+https://github.com/at88mph/fitsio@full-merge#egg=fitsio' 

RUN pip${PYTHON_MAJOR_VERSION} install \
   -e 'git+https://github.com/at88mph/cadctools@fitsio-prototype#egg=cadctools&subdirectory=cadccutout' 

RUN yum -y erase autoconf bzip2-devel gcc gcc-c++ git make openssl-devel perl

# RUN yum -y install java-1.8.0-openjdk-headless fits2wcs

## common cleanup to make image smaller
RUN yum clean all 

# setup
RUN mkdir -p /var/home /usr/cadc/local/bin.x86_rh7_64_opt /usr/cadc/local/cgi/proxies && \
    groupadd -g 30096 cadcops && \
    useradd -u 30297 -g 30096 -d /var/home/cadcops cadcops && \
    usermod -a -G cadcops apache && \
    chown cadcops:cadcops /var/lib/dav && \
    chmod 755 /run/httpd
    
# code
## would prefer to yum install from CADC yum repo in the Dockerfile
COPY build/* /usr/local/bin/

COPY conf/* /etc/httpd/conf/

COPY conf.d/* /etc/httpd/conf.d/

COPY src/adcpbits.sh /usr/cadc/local/cgi/proxies/

## pick the fslice2 implementation to use
COPY src/fslice2-cadccutout.sh /usr/cadc/local/cgi/proxies/fslice2

CMD ["httpd", "-D", "FOREGROUND"]