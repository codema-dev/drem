#!/bin/zsh

poetry install --no-dev
jupyter contrib nbextension install --user
jupyter nbextension enable hide_input_all/main
