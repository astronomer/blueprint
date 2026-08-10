FROM astrocrpublic.azurecr.io/runtime:3.3

# A real consumer repo would list `acme-blueprints` in requirements.txt and
# install it from a package index. Here it is a sibling directory in this
# repository, so it is installed from source.
USER root
RUN pip install --no-deps -e ./package
USER astro
