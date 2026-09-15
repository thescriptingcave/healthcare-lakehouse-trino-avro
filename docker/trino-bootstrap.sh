#!/bin/bash
# Render Trino catalog properties from templates before starting Trino.
#
# Trino treats ${VAR} in catalog files as a secret-provider reference at node
# startup, so credentials cannot be environment-substituted inside the files
# themselves. Instead the templates below are mounted read-only and rendered
# here into /etc/trino/catalog using values from the container environment
# (sourced from .env via docker-compose). Only variables that actually appear
# in a template are substituted, so passwords containing "/" or "&" are safe.

set -euo pipefail

src_dir=/etc/trino/templates
out_dir=/etc/trino/catalog

mkdir -p "${out_dir}"

for tmpl in "${src_dir}"/*.properties.tmpl; do
    name="$(basename "${tmpl}" .tmpl)"
    content="$(cat "${tmpl}")"

    # Extract the explicit ${VAR} references used by this template (sorted
    # longest-first so overlapping names substitute correctly).
    refs="$(printf '%s\n' "${content}" | grep -oE '\$\{[A-Za-z_][A-Za-z0-9_]*\}' | sort -u || true)"

    for ref in ${refs}; do
        var="${ref#\$\{}"
        var="${var%\}}"
        val="${!var:-}"
        content="${content//${ref}/${val}}"
    done

    printf '%s\n' "${content}" > "${out_dir}/${name}"
    echo "Rendered ${out_dir}/${name}"
done

exec /usr/lib/trino/bin/run-trino "$@"