---
jupyter:
  jupytext:
    formats: ipynb,py:percent,md
    text_representation:
      extension: .md
      format_name: markdown
      format_version: '1.2'
      jupytext_version: 1.6.0
  kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

# To run the `drem` etl flow


1. <span style="color:red"> Register your email address with SEAI </span> at https://ndber.seai.ie/BERResearchTool/Register/Register.aspx

2. Select `Cell > Run All` from the dropdown menu

```python
email_address = input("Enter your email address: ")
```

```python
import toml

toml_string = f"""
[context.secrets]
email_address = '{email_address}'

[flow]
checkpointing = true
"""
parsed_toml = toml.loads(toml_string)
with open("prefect-config.toml", "w") as toml_file:
    toml.dump(parsed_toml, toml_file)
```

```python
!python src/drem/etl/residential.py
```
