# Package upload

## Build setup

```
python setup.py sdist bdist_wheel
```

## Tests

```
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

## Oficial

```
twine upload --repository-url https://pypi.org/legacy/ dist/*

twine upload dist/*
```

## Install from tests

```
pip install -i https://test.pypi.org/simple/ avro-object-pkg-guionardo==0.0.3
```