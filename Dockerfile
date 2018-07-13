FROM centos:7

RUN yum install -y epel-release
RUN yum install -y yum install https://centos7.iuscommunity.org/ius-release.rpm

RUN set -x \
    && yum -y install \
        libstdc++ \
        libstdc++-static \
        readline \
        openssl \
        yaml \
        lz4 \
        binutils \
        ncurses \
        libgomp \
        lua \
        curl \
        tar \
        zip \
        unzip \
        libunwind \
        libcurl \
    && yum -y install \
        perl \
        gcc-c++ \
        cmake \
        libyaml-devel \
        lz4-devel \
        binutils-devel \
        lua-devel \
        make \
        git \
        autoconf \
        automake \
        libtool \
        wget


RUN yum -y install ncurses-static readline-static zlib-static pcre-static glibc-static

RUN set -x && \
    cd / && \
    curl -O -L https://www.openssl.org/source/openssl-1.1.0h.tar.gz && \
    tar -xvf openssl-1.1.0h.tar.gz && \
    cd openssl-1.1.0h && \
    ./config no-shared && \
    make && make install

RUN set -x && \
    cd / && \
    git clone https://github.com/curl/curl.git && \
    cd curl && \
    git checkout curl-7_61_0 && \
    ./buildconf && \
    LIBS="-ldl" ./configure --enable-static --disable-shared --with-ssl && \
    make -j && make install

RUN set -x &&\
    cd / && \
    curl -O http://download.icu-project.org/files/icu4c/62.1/icu4c-62_1-src.tgz && \
    tar -xvf icu4c-62_1-src.tgz && \
    cd icu/source && \
    ./configure --with-data-packaging=static --enable-static --disable-shared && \
    make && make install

RUN set -x && \
    cd / && \
    curl -O -L http://download.savannah.nongnu.org/releases/libunwind/libunwind-1.3-rc1.tar.gz && \
    tar -xvf libunwind-1.3-rc1.tar.gz && \
    cd libunwind-1.3-rc1 && \
    ./configure --enable-static --disable-shared && \
    make && make install

RUN set -x && \
    cd / && \
    git clone https://github.com/tarantool/tarantool.git && \
    cd tarantool && \
    git checkout k.nazarov/static-build && \
    git submodule init && \
    git submodule update


RUN set -x \
    && (cd /tarantool; \
       cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo\
             -DENABLE_BUNDLED_LIBYAML:BOOL=ON\
             -DENABLE_BACKTRACE:BOOL=ON\
             -DENABLE_DIST:BOOL=ON\
             .) \
    && make -C /tarantool -j
