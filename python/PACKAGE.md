# Package upload

## Build setup

``` sh
python setup.py sdist bdist_wheel
```

## Tests

``` sh
twine upload --repository-url https://test.pypi.org/legacy/ dist/*
```

## Oficial

``` sh
twine upload --repository-url https://pypi.org/legacy/ dist/*

twine upload dist/*
```

## Install from tests

``` sh
pip install -i https://test.pypi.org/simple/ avro-object-pkg-guionardo==0.9.0
```
