FROM python:3.8-slim

ENV HOME=/usr/local/lib

# Install & build conda
# Source: https://hub.docker.com/r/continuumio/miniconda3/dockerfile
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    wget
ENV PATH=/opt/conda/bin:$PATH
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O ~/miniconda.sh && \
    /bin/bash ~/miniconda.sh -b -p /opt/conda && \
    rm ~/miniconda.sh && \
    /opt/conda/bin/conda clean -tipsy && \
    ln -s /opt/conda/etc/profile.d/conda.sh /etc/profile.d/conda.sh && \
    echo ". /opt/conda/etc/profile.d/conda.sh" >> ~/.bashrc && \
    echo "conda activate base" >> ~/.bashrc

# Install & build Poetry
# Source: https://github.com/airdock-io/docker-python-poetry
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    curl
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python \
    && rm -rf /var/lib/apt/lists/*
ENV PATH=$HOME/.poetry/bin:$PATH

# Install & build zsh dev environment
# Source: https://gist.github.com/MichalZalecki/4a87880bbe7a3a5428b5aebebaa5cd97
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    wget \
    git \
    zsh \
    fonts-powerline
RUN wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true \
    && git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k

# Download codema-dev dotiles to configure zsh shell
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    git
RUN git clone https://github.com/codema-dev/codema-dev-dotfiles $HOME/codema-dev-dotfiles/ \
    && cp -r $HOME/codema-dev-dotfiles/. $HOME \
    && rm -fr $HOME/codema-dev-dotfiles

# Install requirements for Jupyter Notebooks
# Source: https://u.group/thinking/how-to-put-jupyter-notebooks-in-a-dockerfile/
# Add Tini. Tini operates as a process subreaper for jupyter. This prevents kernel crashes.
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    wget \
    curl \
    sudo \
    bzip2 \
    ca-certificates \
    git
ENV TINI_VERSION v0.19.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

# Install gcc to compile C-libraries (such as numpy)
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    gcc

# Install graphviz for prefect flow visualization
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    graphviz

# Install vim for command line text editing
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    vim

# Install nano for easier command line text editing
RUN apt-get update \
    && apt-get install --no-install-recommends -y \
    nano

RUN chsh -s $(which zsh)

ENTRYPOINT ["zsh"]
