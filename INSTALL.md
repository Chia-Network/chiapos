### Package
sudo python3 -m build --sdist --wheel --outdir dist .

### Publish
twine upload --non-interactive --skip-existing --verbose 'dist/*'
