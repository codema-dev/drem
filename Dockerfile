# Install & build Poetry
# Source: https://github.com/airdock-io/docker-python-poetry
FROM python:3.8-slim AS build-poetry

ENV HOME=/usr/local/lib

RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python \
    && rm -rf /var/lib/apt/lists/*

ENV PATH=$HOME/.poetry/bin:$PATH

# Install & build zsh dev environment
# Source: https://gist.github.com/MichalZalecki/4a87880bbe7a3a5428b5aebebaa5cd97
FROM python:3.8-slim AS build-dev-env

ENV HOME=/usr/local/lib

COPY --from=build-poetry /usr/local/lib/.poetry /usr/local/lib/.poetry

ENV PATH=$HOME/.poetry/bin:$PATH

RUN apt-get update \
    && apt-get install -y \
    sudo \
    curl \
    wget \
    git-core \
    vim \
    zsh \
    fonts-powerline \
    gcc

RUN wget https://github.com/robbyrussell/oh-my-zsh/raw/master/tools/install.sh -O - | zsh || true \
    && git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k

RUN git clone https://github.com/codema-dev/codema-dev-dotfiles $HOME/codema-dev-dotfiles/ \
    && cp -r $HOME/codema-dev-dotfiles/. $HOME \
    && rm -fr $HOME/codema-dev-dotfiles

RUN chsh -s $(which zsh)
