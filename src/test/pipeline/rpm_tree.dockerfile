FROM centos:7

# Getting the pre-requirments for building the rpms
RUN yum install -y -q tree wget dnf vim && \
    yum clean all
RUN dnf install -y -q rpm-build rpm-devel rpmlint rpmdevtools && \
    yum clean all
# Setting the rpm tree
RUN rpmdev-setuptree

# Getting go 1.12.7
RUN version="1.12.7" && \
    wget https://dl.google.com/go/go${version}.linux-amd64.tar.gz && \
    tar -xzf go${version}.linux-amd64.tar.gz && \
    mv go /usr/local

# Setting the env variables
ENV GOROOT=/usr/local/go
ENV GOPATH=$HOME/rpmbuild/BUILD
ENV PATH=$GOPATH/bin:$GOROOT/bin:$PATH
