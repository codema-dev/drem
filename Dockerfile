# Install & build Poetry
# Source: https://github.com/airdock-io/docker-python-poetry
FROM python:3.8-slim

ENV HOME=/usr/local/lib

RUN apt-get update \
    && apt-get install --no-install-recommends -y curl

# Install & build Poetry
# Source: https://github.com/airdock-io/docker-python-poetry
RUN curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python \
    && rm -rf /var/lib/apt/lists/*

ENV PATH=$HOME/.poetry/bin:$PATH

# Install & build zsh dev environment
# Source: https://gist.github.com/MichalZalecki/4a87880bbe7a3a5428b5aebebaa5cd97
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

# Download codema-dev dotiles to configure zsh shell
RUN git clone https://github.com/codema-dev/codema-dev-dotfiles $HOME/codema-dev-dotfiles/ \
    && cp -r $HOME/codema-dev-dotfiles/. $HOME \
    && rm -fr $HOME/codema-dev-dotfiles

# Install requirements for Jupyter Notebooks
# Source: https://u.group/thinking/how-to-put-jupyter-notebooks-in-a-dockerfile/
# Add Tini. Tini operates as a process subreaper for jupyter. This prevents kernel crashes.
ENV TINI_VERSION v0.6.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini

RUN chsh -s $(which zsh)

ENTRYPOINT ["zsh"]
